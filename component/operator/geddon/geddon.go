package geddon

import (
	"athena"
	"athena/component/operator/geddon/plugin"
	"athena/event"
	"athena/properties"
	"fmt"
	"gopkg.in/tomb.v2"
	"sync"
	"time"
)

const idleTimeoutSeconds = int64(30)

var (
	StatistTypeProperty = properties.NewProperty[string]("statist-type", "geddon statist type, eg: nginx squid all.", "all")
)

func formatId(t time.Time) string {
	year, month, day := t.Date()
	return fmt.Sprintf("%d%02d%02d%02d%02d%02d-%d", year, month, day, t.Hour(), t.Minute(), t.Second(), time.Now().UnixNano())
}

type operator struct {
	sync.Mutex
	ctx               athena.Context
	tickerLife        tomb.Tomb
	next              athena.EmitNext
	newBulkPluginFunc plugin.NewBundleFunc
	bundleMap         map[int64]plugin.Bundle
	lastWrite         map[int64]int64
	firstWrite        map[int64]int64
	hardTimeoutSec    int64
}

func (o *operator) Open(ctx athena.Context) error {
	o.ctx = ctx
	switch o.ctx.Properties().GetString(StatistTypeProperty.Type()) {
	case "squid":
		o.newBulkPluginFunc = newSquidBundle
	case "nginx":
		o.newBulkPluginFunc = newNginxBundle
	default:
		o.newBulkPluginFunc = newAllBundle
	}
	o.bundleMap = map[int64]plugin.Bundle{}
	o.tickerLife = tomb.Tomb{}
	o.lastWrite = map[int64]int64{}
	o.firstWrite = map[int64]int64{}
	return nil
}

func (o *operator) Close() error {
	o.tickerLife.Kill(nil)
	if o.bundleMap != nil {
		for ti, bulkPlugin := range o.bundleMap {
			bulkPlugin.End(formatId(time.Unix(ti, 0)))
		}
	}
	o.tickerLife.Kill(nil)
	return o.tickerLife.Wait()
}

func (o *operator) PropertyDef() athena.PropertyDef {
	return athena.PropertyDef{StatistTypeProperty}
}

func (o *operator) Emit(ptr event.Ptr) {
	o.Lock()
	defer o.Unlock()
	now := time.Now().Unix()

	if ptr != nil {
		ti := ptr.Time.Truncate(time.Minute).Unix()

		if o.bundleMap[ti] == nil {
			o.bundleMap[ti] = o.newBulkPluginFunc(o.ctx, o.next)
			if o.firstWrite != nil {
				o.firstWrite[ti] = now
			}
		}
		o.bundleMap[ti].Add(ptr)
		o.lastWrite[ti] = now
		return
	}

	for ti, bundle := range o.bundleMap {
		if o.lastWrite[ti]+idleTimeoutSeconds < now || o.firstWrite != nil && o.firstWrite[ti]+o.hardTimeoutSec < now {
			bundle.End(formatId(time.Unix(ti, 0)))
			delete(o.bundleMap, ti)
			delete(o.lastWrite, ti)
			if o.firstWrite != nil {
				delete(o.firstWrite, ti)
			}
		}
	}
}

func (o *operator) Collect(next athena.EmitNext) error {
	o.next = next
	o.tickerLife.Go(func() error {
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				o.Emit(nil)
			case <-o.tickerLife.Dying():
				return nil
			}
		}
	})
	<-o.ctx.Done()
	return nil
}

func New() athena.Operator {
	return &operator{}
}
