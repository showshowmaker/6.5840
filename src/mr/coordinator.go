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
	files   []string
	NReduce int
	//0: not started, 1: in progress, 2: done
	mapStates    []int
	reduceStates []int
	mapDone      bool
	reduceDone   bool

	FileNamesMid []string

	mapStartTimes    []time.Time
	reduceStartTimes []time.Time
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AllocateJob(args *Request, reply *Response) error {
	// Your code here.
	reply.NReduce = c.NReduce
	if !c.mapDone {
		for i, state := range c.mapStates {
			if state == 0 {
				reply.HasJob = true
				reply.JobType = MapJob
				reply.MapJobID = i
				reply.FileName = c.files[i]
				// if reply.FileName == "" {
				// 	fmt.Println("filename is empty ", i)
				// }
				// fmt.Println(reply.FileName, " ", i)
				reply.NReduce = c.NReduce
				c.mapStates[i] = 1
				c.mapStartTimes[i] = time.Now()
				return nil
			}
		}
	} else if !c.reduceDone {
		// c.reduceDone = true
		// return nil
		for i, state := range c.reduceStates {
			if state == 0 {
				reply.HasJob = true
				reply.JobType = ReduceJob
				reply.ReduceJobID = i
				reply.FileNameMid = c.FileNamesMid
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
	reply.Stop = false
	reply.NReduce = c.NReduce
	if args.JobType == MapJob {
		c.mapStates[args.JobID] = 2
		c.FileNamesMid = append(c.FileNamesMid, fmt.Sprintf("%d", args.JobID))
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

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) timeoutHandler() {
	// Your code here.
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

	// Your code here.
	c.files = files
	c.NReduce = NReduce
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
