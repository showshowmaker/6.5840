package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		//无限循环待机，只有在coordinator通知任务已经全部完成之后才会退出
		reply := CallForJob()
		//说明可以退出
		if reply == nil || reply.JobType == AllFinished {
			return
		}
		// fmt.Println("NReduce: ", reply.NReduce)
		// if reply.NReduce == 0 {
		// 	return
		// }
		//没有job就待机一段时间再次请求
		if !reply.HasJob {
			time.Sleep(1 * time.Second)
			continue
		}
		if reply.JobType == MapJob {
			// fmt.Println("map ", reply.FileName)
			//mapjob，参考mrsequential
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("map cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			kva := mapf(reply.FileName, string(content))
			files := make([]*os.File, reply.NReduce)
			encoders := make([]*json.Encoder, reply.NReduce)
			midFileNames := make([]string, reply.NReduce)
			for i := 0; i < reply.NReduce; i++ {
				midFileNames[i] = fmt.Sprintf("mr-%d-%d", reply.MapJobID, i)
				file, err := os.Create(midFileNames[i])
				if err != nil {
					log.Fatalf("cannot create %v", midFileNames[i])
				}
				files[i] = file
				encoders[i] = json.NewEncoder(file)
			}
			for _, kv := range kva {
				// if reply.NReduce == 0 {
				// 	fmt.Println("NReduce is 0")
				// }
				err := encoders[ihash(kv.Key)%reply.NReduce].Encode(&kv)
				if err != nil {
					log.Fatalf("cannot encode %v", kv)
				}
			}
			for i := 0; i < reply.NReduce; i++ {
				files[i].Close()
			}
			//任务完成，通知coordinator
			reply = CallForFin(reply.JobType, reply.MapJobID)
			if reply.Stop {
				return
			}
		} else if reply.JobType == ReduceJob {
			//reducejob，参考mrsequential
			reduceId := reply.ReduceJobID
			// fmt.Println("reduce ", filename)
			files := make([]*os.File, len(reply.FileNameMid))
			decoders := make([]*json.Decoder, len(reply.FileNameMid))
			for i, mid := range reply.FileNameMid {
				filename := fmt.Sprintf("mr-%s-%d", mid, reduceId)
				// fmt.Println("reduce ", filename)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("reduce cannot open %v", file)
				}
				files[i] = file
				decoders[i] = json.NewDecoder(file)
			}
			intermediate := []KeyValue{}
			for _, decoder := range decoders {
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))
			// fmt.Printf("intermediate len %d\n", len(intermediate))
			oname := fmt.Sprintf("mr-out-%d", reduceId)
			ofile, _ := os.Create(oname)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			reply = CallForFin(reply.JobType, reply.ReduceJobID)
			if reply.Stop {
				return
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// worker请求分配job
func CallForJob() *Response {
	args := Request{}
	reply := Response{}
	ok := call("Coordinator.AllocateJob", &args, &reply)
	if ok {
		// fmt.Println(reply)
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

// worker完成job之后通知coordinator
func CallForFin(jobType JobType, jobID int) *Response {
	args := Request{}
	args.JobType = jobType
	args.JobID = jobID
	reply := Response{}
	ok := call("Coordinator.FinishJob", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// bash test-mr.sh
// bash test-mr-many.sh 10
// bash mytest.sh
