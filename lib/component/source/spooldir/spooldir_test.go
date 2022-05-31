package spooldir

import (
	"testing"
)

func TestSource(t *testing.T) {
	////watcher, err := fsnotify.NewWatcher()
	////err = watcher.Add("/Users/klein/Development/go/src/connector/")
	////if err != nil {
	////	panic(err)
	////}
	////for {
	////	select {
	////	case e, ok := <-watcher.Events:
	////		if !ok {
	////			println(ok)
	////		}
	////		if e.Op&fsnotify.Create == fsnotify.Create {
	////			println(e.Name)
	////		}
	////		//ignore
	////	case innerErr, ok := <-watcher.Errors:
	////		if !ok {
	////			err = innerErr
	////
	////		}
	////	}
	////}
	//source := New()
	//v := viper.New()
	//v.Set("scan", "/Users/klein/Temp/123")
	//v.Set("afterCombine", "/Users/klein/Temp/123/1234")
	//v.Set("ignore-pattern", "")
	//v.Set("file-suffix", ".bak")
	//ctx := context.New(_c.Background(), v, logrus.New())
	//_ = source.Init(context.NewComponentContext(ctx, state.CreateOrGetState("")))
	//_ = source.OnEvent(func(event event.Ptr) {
	//	fmt.Printf("%+v\n", event)
	//})
}
