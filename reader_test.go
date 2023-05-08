package logstream

import (
	"fmt"
	"os"
	"os/signal"
	"testing"
)

func TestReader_Start(t *testing.T) {
	var (
		readStream *Reader
		err        error
	)
	readStream, err = NewReader("", func(items []*PoppedMsgItem) error {
		fmt.Println("consume " + items[0].Topic)
		for _, item := range items {
			fmt.Println(string(item.Data))
			readStream.ConfirmMsg(item.Topic, item.Seq, item.IdxOffset)
		}
		fmt.Println(items[0].Topic+" batch consumed", len(items))
		return nil
	})
	if err != nil {
		panic(err)
	}

	go func() {
		signCh := make(chan os.Signal)
		signal.Notify(signCh)
		<-signCh
		readStream.Exit()
	}()

	readStream.Start()
}
