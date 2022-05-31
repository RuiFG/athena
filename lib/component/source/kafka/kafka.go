package kafka

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/log"
	"athena/lib/properties"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"time"
	"unsafe"
)

var (
	TopicsProperty                = properties.NewRequiredProperty[[]string]("topics", "")
	VersionProperty               = properties.NewProperty[string]("version", "", "2.4.0")
	BrokersProperty               = properties.NewRequiredProperty[[]string]("brokers", "")
	ClientIdProperty              = properties.NewProperty[string]("client.id", "client id", "")
	GroupIdProperty               = properties.NewProperty[string]("group.id", "", "agg")
	OffsetsCommitIntervalProperty = properties.NewProperty[int]("offsets.commit.interval", "kafka commit interval sec", 5)
	OffsetsInitial                = properties.NewProperty[string]("offsets.initial", "newest or oldest", "oldest")

	SASLUserProperty     = properties.NewProperty[string]("sasl-username", "", "")
	SASLPasswordProperty = properties.NewProperty[string]("sasl-password", "", "")
)

type source struct {
	ctx           athena.Context
	logger        athena.Logger
	emitNext      athena.EmitNext
	consumerGroup sarama.ConsumerGroup
}

func (s *source) Open(ctx athena.Context) error {
	s.ctx = ctx
	s.logger = log.Ctx(s.ctx)
	//TODO all kafka consumer properties

	config := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(s.ctx.Properties().GetString(VersionProperty))
	if err != nil {
		return err
	}
	config.Version = version
	//sasl
	saslUser := s.ctx.Properties().GetString(SASLUserProperty)
	saslPassword := s.ctx.Properties().GetString(SASLPasswordProperty)
	if saslUser != "" && saslPassword != "" {
		config.Net.SASL.User = saslUser
		config.Net.SASL.Password = saslPassword
		config.Net.SASL.Enable = true
	}
	config.Consumer.Return.Errors = true
	//OffsetNewest or OffsetOldest.
	config.Consumer.Offsets.AutoCommit.Interval = time.Duration(s.ctx.Properties().GetInt(OffsetsCommitIntervalProperty)) * time.Second
	if s.ctx.Properties().GetString(OffsetsInitial) == "newest" {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	//clientId
	clientId := s.ctx.Properties().GetString(ClientIdProperty)
	if clientId != "" {
		config.ClientID = clientId
	}

	s.consumerGroup, err = sarama.NewConsumerGroup(s.ctx.Properties().GetStringSlice(BrokersProperty), s.ctx.Properties().GetString(GroupIdProperty), config)
	if err != nil {
		return err
	}
	go s.handleErrors()
	return nil
}

func (s *source) Close() error {
	var err error
	for i := 1; i < 4; i++ {
		err = s.consumerGroup.Close()
		if err != nil {
			s.logger.Warnw("close kafka consumer error, waiting 1 second.", "time", i, "err", err)
			time.Sleep(1 * time.Second)
		} else {
			return nil
		}
	}
	return errors.WithMessage(err, "can't close kafka consumer")
}

func (s *source) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{TopicsProperty, VersionProperty, BrokersProperty, GroupIdProperty, OffsetsCommitIntervalProperty, OffsetsInitial}
}

func (s *source) Collect(emitNext athena.EmitNext) error {
	s.emitNext = emitNext
	for {
		var err error
		select {
		case <-s.ctx.Done():
			return nil
		default:
			err = s.consumerGroup.Consume(s.ctx.Ctx(), s.ctx.Properties().GetStringSlice(TopicsProperty), s)
			if err != nil {
				return errors.WithMessage(err, "can't collect kafka")
			}
		}
	}

}

func (s *source) Setup(_ sarama.ConsumerGroupSession) error {
	s.logger.Infof("set up...")
	return nil
}

func (s *source) Cleanup(_ sarama.ConsumerGroupSession) error {
	s.logger.Infof("clean up...")
	return nil
}

func (s *source) handleErrors() {
	// consume errors
	go func() {
		for err := range s.consumerGroup.Errors() {
			select {
			case <-s.ctx.Done():
				s.logger.Infof("shutdown handle errors.")
				return
			default:
				s.logger.Errorw("received error.", "err", err)
			}
		}
	}()
}

func (s *source) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		headers := map[string]string{}
		for _, recordHeader := range message.Headers {
			headers[string(recordHeader.Key)] = string(recordHeader.Value)
		}
		s.emitNext(
			&athena.Event{
				Meta: map[string]any{
					"topic":     message.Topic,
					"partition": message.Partition,
					"offset":    message.Offset,
					"timestamp": message.Timestamp},
				Message: map[string]any{
					"value":   *(*string)(unsafe.Pointer(&message.Value)),
					"key":     *(*string)(unsafe.Pointer(&message.Key)),
					"headers": headers,
				},
				Time: time.Now()}, func() {
				session.MarkMessage(message, "")
			})
	}
	return nil
}

func New() athena.Source {
	return &source{}
}

func init() {
	component.RegisterNewSourceFunc("kafka", New)
}
