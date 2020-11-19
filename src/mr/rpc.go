package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

type JobArgs struct {
	A int
}

const (
	QUIT = iota
	WAIT
	MAP
	REDUCE
)

type JobReply struct {
	Jtype  int
	Jfiles []string
	Jid    int
	Jnum   int
}

type EmptyReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
