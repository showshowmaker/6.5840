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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type JobType int

const (
	MapJob      JobType = 1
	ReduceJob   JobType = 2
	AllFinished JobType = 3
)

type Request struct {
	JobType JobType
	JobID   int
}

type Response struct {
	JobType     JobType
	MapJobID    int
	ReduceJobID int
	FileName    string
	NReduce     int
	HasJob      bool

	FileNameMid []string

	Stop bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
