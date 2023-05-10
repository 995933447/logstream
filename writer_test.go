package logstream

import (
	"fmt"
	"testing"
)

func TestWriter_Write(t *testing.T) {
	writer, _ := NewWriter("")
	for i := 0; i < 10; i++ {
		writer.Write("test_topic", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
}

func TestWriter_Flush(t *testing.T) {
	writer, err := NewWriter("")
	if err != nil {
		t.Log(err)
		return
	}
	for i := 0; i < 2; i++ {
		writer.Write("test_topic2", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	writer.Flush()
}

func TestWriter_Exit(t *testing.T) {
	writer, _ := NewWriter("")
	for i := 0; i < 10; i++ {
		writer.Write("test_topic3", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	writer.Exit()
}

func TestWriter_Stop(t *testing.T) {
	writer, _ := NewWriter("")
	for i := 0; i < 10; i++ {
		writer.Write("test_topic4", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	writer.Stop()
}

func TestWriter_Resume(t *testing.T) {
	writer, _ := NewWriter("")
	for i := 0; i < 10000; i++ {
		writer.Write("test_topic5", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	writer.Stop()
	err := writer.Resume()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10000; i++ {
		writer.Write("test_topic5", []byte(fmt.Sprintf("hello world:%d times", i)))
	}
	writer.Exit()
	err = writer.Resume()
	if err != nil {
		t.Fatal(err)
	}
}
