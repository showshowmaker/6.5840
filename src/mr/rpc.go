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
	//在worker完成job之后，需要更新job的完成状态
	JobType JobType
	JobID   int
}

type Response struct {
	//job类型，worker根据这个类型来判断是map还是reduce，进入不同的分支
	JobType JobType
	//是否有job，如果没有job，worker进入待机状态会sleep一段时间再次请求
	HasJob bool
	//是否可以停止wrker，为ture表明所有任务都已经完成
	Stop bool

	//mapjob需要的信息
	MapJobID int
	FileName string
	NReduce  int

	//reducejob需要的信息
	ReduceJobID int
	//map生成的中间文件的名字的中间字段，例如mr-0-1，FileNameMid中会存储0，在worker的reduce执行阶段会依据这个中间字段来生产该reduce需要的文件名
	FileNameMid []string
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
