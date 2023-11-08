package dLog

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

type logTopic string

const (
	DClient  logTopic = "CLNT"
	DCommit  logTopic = "CMIT"
	DDrop    logTopic = "DROP"
	DError   logTopic = "ERRO"
	DInfo    logTopic = "INFO"
	DLeader  logTopic = "LEAD"
	DPersist logTopic = "PERS"
	DSnap    logTopic = "SNAP"
	DTerm    logTopic = "TERM"
	DTest    logTopic = "TEST"
	DTimer   logTopic = "TIMR"
	DTrace   logTopic = "TRCE"
	DVote    logTopic = "VOTE"
	DAppend  logTopic = "APND"
	DWarn    logTopic = "WARN"
	DLock    logTopic = "LOCK"
)

var rwMutex sync.RWMutex
var debugStart time.Time
var debugVerbosity int

func Init() {
	rwMutex.Lock()
	defer rwMutex.Unlock()
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

func Debug(topic logTopic, format string, a ...interface{}) {
	rwMutex.RLock()
	defer rwMutex.RUnlock()
	if debugVerbosity >= 1 {
		nanoseconds := time.Since(debugStart).Nanoseconds()
		prefix := fmt.Sprintf("%03d,%03d,%03d,%03d %v ",
			nanoseconds/1e9, nanoseconds%1e9/1e6, nanoseconds%1e6/1e3, nanoseconds%1e3,
			string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}
