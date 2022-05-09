package kafka

import (
	"athena"
	"athena/event"
	"athena/properties"
	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
	"time"
	"unsafe"
)

var (
	TopicsProperty                = properties.NewRequiredProperty[[]string]("topics", "")
	VersionProperty               = properties.NewProperty[string]("version", "", "2.4.0")
	BrokersProperty               = properties.NewRequiredProperty[[]string]("brokers", "")
	GroupIdProperty               = properties.NewProperty[string]("group.id", "", "agg")
	OffsetsCommitIntervalProperty = properties.NewProperty[int]("offsets.commit.interval", "kafka commit interval sec", 5)
	OffsetsInitial                = properties.NewProperty[string]("offsets.initial", "newest or oldest", "oldest")
)

type source struct {
	ctx athena.Context

	logger logrus.FieldLogger

	consumerGroup sarama.ConsumerGroup
}

func (s *source) Open(ctx athena.Context) error {
	s.ctx = ctx
	//TODO all kafka consumer properties
	//OffsetNewest or OffsetOldest.
	config := sarama.NewConfig()
	version, err := sarama.ParseKafkaVersion(s.ctx.Properties().GetString(VersionProperty.Name()))
	if err != nil {
		return err
	}
	config.Consumer.Return.Errors = true

	config.Version = version
	config.Consumer.Offsets.AutoCommit.Interval = time.Duration(s.ctx.Properties().GetInt(OffsetsCommitIntervalProperty.Name())) * time.Second
	if s.ctx.Properties().GetString(OffsetsInitial.Name()) == "newest" {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	s.consumerGroup, err = sarama.NewConsumerGroup(s.ctx.Properties().GetStringSlice(BrokersProperty.Name()), s.ctx.Properties().GetString(GroupIdProperty.Name()), config)
	if err != nil {
		return err
	}
	return nil
}

func (s *source) Close() error {
	return nil
}

func (s *source) PropertyDef() athena.PropertyDef {
	return athena.PropertyDef{TopicsProperty, VersionProperty, BrokersProperty, GroupIdProperty, OffsetsCommitIntervalProperty, OffsetsInitial}
}

func (s *source) Collect(emitNext athena.EmitNext) error {
	for {
		var err error
		select {
		case <-s.ctx.Done():
			s.logger.Info("ctx is done, close kafka consumer.")
			for i := 1; i < 4; i++ {
				err = s.consumerGroup.Close()
				if err != nil {
					s.logger.WithError(err).WithField("time", i).Warn("close kafka consumer error, waiting 1 second.")
					time.Sleep(1 * time.Second)
				} else {
					return nil
				}
			}
			s.logger.Error("close kafka consumer error, stop retry.")
			return err
		default:
			err = s.consumerGroup.Consume(s.ctx.Ctx(), s.ctx.Properties().GetStringSlice(TopicsProperty.Name()), &consumer{emitNext: emitNext})
			if err != nil {
				s.logger.WithError(err).Warn("collect kafka error, stopping collect.")
				return err
			}
		}
	}

}

type consumer struct {
	emitNext athena.EmitNext
}

func (c *consumer) Setup(session sarama.ConsumerGroupSession) error {
	//TODO recovery offset
	return nil
}

func (c *consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		headers := map[string]string{}
		for _, recordHeader := range message.Headers {
			headers[string(recordHeader.Key)] = string(recordHeader.Value)
		}
		c.emitNext(event.MustNewPtr(map[string]interface{}{
			"topic":     message.Topic,
			"partition": message.Partition,
			"offset":    message.Offset,
			"timestamp": message.Timestamp,
		}, map[string]interface{}{
			"value":   *(*string)(unsafe.Pointer(&message.Value)),
			"key":     *(*string)(unsafe.Pointer(&message.Key)),
			"headers": headers,
		}))
		session.MarkMessage(message, "")
	}
	return nil
}

func New() athena.Source {
	return &source{}
}
