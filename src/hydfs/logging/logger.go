package logging

import (
	nodeID "hydfs-g33/membership/node"
	"log"
	"os"
	"time"
)

type Logger struct {
	nodeID nodeID.NodeID
	l      *log.Logger
}

func NewLogger(nodeID nodeID.NodeID, filePath string) *Logger {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	return &Logger{
		nodeID: nodeID,
		l:      log.New(f, "", 0),
	}
}

func (lg *Logger) Receive(op, file, client string, size int64) {
	t := time.Now().Format(time.RFC3339Nano)
	lg.l.Printf("RECV %s node=%s file=%s client=%s size=%d time=%s",
		op, lg.nodeID.NodeIDToString(), file, client, size, t)
}

func (lg *Logger) Done(op, file string, version uint64, size int64) {
	t := time.Now().Format(time.RFC3339Nano)
	lg.l.Printf("DONE %s node=%s file=%s version=%d size=%d time=%s",
		op, lg.nodeID.NodeIDToString(), file, version, size, t)
}

func (lg *Logger) Infof(format string, v ...interface{}) {
	t := time.Now().Format(time.RFC3339Nano)
	lg.l.Printf("INFO node=%s time=%s "+format,
		append([]interface{}{lg.nodeID.NodeIDToString(), t}, v...)...)
}
