package mock

import (
	"athena/athena"
	"athena/lib/component"
	"athena/lib/properties"
	"time"
)

var (
	IntervalProperty = properties.NewProperty[int]("interval", "random source generate record interval", 100)
)

type source struct {
	ctx      athena.Context
	interval int
}

func (s *source) PropertiesDef() athena.PropertiesDef {
	return athena.PropertiesDef{IntervalProperty}
}

func (s *source) Collect(emitNext athena.EmitNext) error {
	//open source
	ticker := time.NewTicker(time.Duration(s.interval) * time.Millisecond)
	for true {
		select {
		case <-s.ctx.Done():
			//source close
			return nil
		case <-ticker.C:
			emitNext(
				&athena.Event{
					Message: `dx-otherofchina-other-1-10-101-1-145 58.44.196.169 st.dl.pinyuncloud.com "application/x-steam-chunk" [06/Jan/2022:11:32:12 +0800] "GET http://st.dl.pinyuncloud.com/depot/578081/chunk/305816807006b3ee9a5e4fb3b7da07bae1a889a7 HTTP/1.1" 200 1015545 "-" "Valve/Steam+HTTP+Client+1.0" 6 1014848 "1014848" "-@1014848" "-" 58.44.196.169:1191@-@-@BC118_HK-xianggang-xianggang-4-cache-2,+BC10_yd-guangdong-jiangmen-1-cache-1,+BC230_dx-lt-yd-jiangsu-huaian-8-cache-11,+BC48_dx-zhejiang-jiaxing-9-cache-4,+BC34_dx-hubei-xiangyang-11-cache-2,+BC[205202693570010410000354]_henan-dx@-@b56d4847c743d5affca59f9a765e04ec@-@-@-@-@-@-@-@-@-@-@-@-@- -@-@-@-@0@-@0@-@-@-@-@-@-@242 "-" - HIT NONE -`,
				}, nil)

		}
	}
	return nil
}

func (s *source) Open(ctx athena.Context) error {
	s.ctx = ctx
	s.interval = ctx.Properties().GetInt(IntervalProperty)

	return nil
}

func (s *source) Close() error {
	return nil
}

//New uses for test only
func New() athena.Source {
	return &source{}
}

func init() {
	component.RegisterNewSourceFunc("mock", New)
}
