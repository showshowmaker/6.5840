package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	//要处理的文件数量，有几个文件就说明有几个map任务
	files []string
	//reduce任务数量
	nReduce int
	//0: not started, 1: in progress, 2: done
	//map和reduce的任务状态，states数组中0表示未开始，1表示进行中，2表示已完成
	//在超时处理中，如果进行中的任务超时，会将state置为0，重新分配任务
	mapStates    []int
	reduceStates []int
	//阶段性判断，map和reduce任务是否完成
	mapDone    bool
	reduceDone bool
	//map生成的中间文件的名字的中间字段，例如mr-0-1，FileNameMid中会存储0，在worker的reduce执行阶段会依据这个中间字段来生产该reduce需要的文件名
	fileNamesMid []string

	//记录map和reduce任务的开始时间，用于超时处理
	mapStartTimes    []time.Time
	reduceStartTimes []time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateJob(args *Request, reply *Response) error {
	// Your code here.
	//先分配map任务，map任务都完成了就分配reduce任务，reduce任务都完成了就返回AllFinished，worker根据这个返回值来判断是否可以退出
	if !c.mapDone {
		for i, state := range c.mapStates {
			if state == 0 {
				reply.HasJob = true
				reply.JobType = MapJob
				reply.MapJobID = i
				reply.FileName = c.files[i]
				reply.NReduce = c.nReduce
				c.mapStates[i] = 1
				c.mapStartTimes[i] = time.Now()
				return nil
			}
		}
	} else if !c.reduceDone {
		for i, state := range c.reduceStates {
			if state == 0 {
				reply.HasJob = true
				reply.JobType = ReduceJob
				reply.ReduceJobID = i
				reply.FileNameMid = c.fileNamesMid
				c.reduceStates[i] = 1
				c.reduceStartTimes[i] = time.Now()
				return nil
			}
		}
	} else {
		reply.JobType = AllFinished
	}
	return nil
}

func (c *Coordinator) FinishJob(args *Request, reply *Response) error {
	// Your code here.
	//worker完成任务后调用这个函数，coordinator以此更新任务状态
	reply.Stop = false
	if args.JobType == MapJob {
		c.mapStates[args.JobID] = 2
		c.fileNamesMid = append(c.fileNamesMid, fmt.Sprintf("%d", args.JobID))
		mapDone := true
		for _, state := range c.mapStates {
			if state != 2 {
				mapDone = false
				break
			}
		}
		if mapDone {
			c.mapDone = true
		}
	} else if args.JobType == ReduceJob {
		c.reduceStates[args.JobID] = 2
		reduceDone := true
		for _, state := range c.reduceStates {
			if state != 2 {
				reduceDone = false
				break
			}
		}
		if reduceDone {
			c.reduceDone = true
			reply.Stop = true
		}
	}
	return nil
}

func (c *Coordinator) timeoutHandler() {
	// Your code here.
	//超市处理，如果任务超时，将任务状态置为0，重新分配任务
	for !c.reduceDone {
		time.Sleep(3 * time.Second)
		for i, state := range c.mapStates {
			if state == 1 {
				if time.Since(c.mapStartTimes[i]) > 10*time.Second {
					c.mapStates[i] = 0
					//fmt.Printf("map job %d timeout\n", i)
				}
			}
		}
		for i, state := range c.reduceStates {
			if state == 1 {
				if time.Since(c.reduceStartTimes[i]) > 10*time.Second {
					c.reduceStates[i] = 0
					//fmt.Printf("reduce job %d timeout\n", i)
				}
			}
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	go c.timeoutHandler()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false
	// Your code here.
	return c.reduceDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, NReduce int) *Coordinator {
	c := Coordinator{}
	//初始化coordinator
	// Your code here.
	c.files = files
	c.nReduce = NReduce
	// fmt.Println("initNReduce: ", NReduce)
	c.mapStates = make([]int, len(files))
	c.reduceStates = make([]int, NReduce)
	c.mapDone = false
	c.reduceDone = false
	c.mapStartTimes = make([]time.Time, len(files))
	c.reduceStartTimes = make([]time.Time, NReduce)
	c.server()
	return &c
}
