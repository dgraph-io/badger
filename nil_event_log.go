package badger

import "golang.org/x/net/trace"

var (
	noEventLog trace.EventLog = nilEventLog{}
)

type nilEventLog struct{}

func (nel nilEventLog) Printf(format string, a ...interface{}) {}

func (nel nilEventLog) Errorf(format string, a ...interface{}) {}

func (nel nilEventLog) Finish() {}
