package logstream

import (
	"github.com/995933447/log-go"
	"github.com/995933447/log-go/impl/loggerwriter"
	"github.com/995933447/std-go/print"
)

var Logger *log.Logger

func init() {
	Logger = log.NewLogger(loggerwriter.NewStdoutLoggerWriter(print.ColorGreen))
}
