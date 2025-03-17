# Lab1 MapReduce

本实验的任务是构建一个MapReduce系统。主要要实现一个Worker工作进程，用于执行map或reduce任务；还要一个Coordinator协调器进程，用于分配map和reduce任务。

代码见[showshowmaker/6.5840](https://github.com/showshowmaker/6.5840/tree/main)

执行*bash test-mr-many.sh 10*，第一次10轮都pass了，第二次在第8轮出现报错，可能存在一些小问题。

![ef6a9fce9891a09ab2ba7c2b9cc459bf](C:\Users\w1j2h\Desktop\study\6.5840\doc\assets\ef6a9fce9891a09ab2ba7c2b9cc459bf.png)

## mrsequential

### 参考mrsequential的实现

在实验开始前，我已经学习了mapreduce的相关知识，知道mapreduce可以用来处理词频统计或倒排索引这样的问题，但是我对其具体代码实现却一片空白，go的语法也欠缺很多。

为了能够尽快上手开始写实验，就先按依照实验指导（[6.5840 实验室 1：MapReduce --- 6.5840 Lab 1: MapReduce](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)）运行了给出的测试代码，并分析mrsequential.go的代码设计（后面证明这还挺重要的，mapreduce具体操作中的文件读写、encoder、decoder之类的都要参考这个的代码来写）。

下面这几行代码中没见过的是第三行，wc.go里面有Map和Reduce（要导出的变量或方法首字母要大写，后面在这里狠狠地吃亏了）两个函数，buildmode选择plugin，将wc.go编译生成了wc.so，之后在mrsequential.go中就可以使用plugin包来调用执行wc.go里面的mapreduce函数。

```sh
cd ~/6.5840
cd src/main
go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrsequential.go wc.so pg*.txt
more mr-out-0
A 509
```

### 对比mrsequential

mrsequential中实现的是单机词频统计的代码，逻辑也比较清晰，先使用loadPlugin从指定文件中读取得到map和reduce函数，之后进行map操作。

map操作如下，对每个文件的内容调用mapf将其转化为键值对（统计每个单词出现的频率，不会进行合并，即值都是“1”），存储到intermediate中，之后还要排序。

```go
intermediate := []mr.KeyValue{}
for _, filename := range os.Args[2:] {
    file, err := os.Open(filename)
    if err != nil {
        log.Fatalf("cannot open %v", filename)
    }
    content, err := ioutil.ReadAll(file)
    if err != nil {
        log.Fatalf("cannot read %v", filename)
    }
    file.Close()
    kva := mapf(filename, string(content))
    intermediate = append(intermediate, kva...)
}
```

reduce操作是将上一步得到的排序好的键值对的键相同的进行合并，即统计每一个键在intermediate中出现的次数作为该键的值。

```go
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

    // this is the correct format for each line of Reduce output.
    fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

    i = j
}
```

## 如何实现mapreduce

### 从mrsequential到mapreduce

我们的mapreduce要实现和mrsequential一样的功能，但是我们的map和reduce要在多个worker上运行，map间并行，reduce间并行，map全部完成后才能开始reduce。

现实的map和reduce是在多个真实的物理节点上运行，有一台节点作为协调器分配任务并管理其他节点，但是我们的实验是在一台机器上模拟的，没有那么多物理节点，所以就用多线程来代替多节点，一个work线程代表一个物理节点，有多个worker线程，还有且只有一个协调器coordinator线程负责任务分配，管理其他节点。真实的场景中实验tcp之类的协议来进行节点间的通信，本实验实验rpc模拟来进行线程之间的通信交互。

注意到mrsequential中mapf和reducef均在for循环中被执行了多次，mapf和reducef均是可以并行执行的，我们要做的就是使其并行执行。

### RPC

参考代码中的示例实现即可，我没有学习其细节实现。以下仅说明如何在本实验中使用rpc。

在rpc.go中定义rpc请求和应答所需的结构体，设计需要的字段。

参照CallExample()在worker.go中设计需要的请求函数。

之后在Worker中调用CallXXX函数向coordinator发送请求并接收响应即可。

在本实验中设计的worker只当rpc请求的发送方，coordinator只当接收方，coordinator也是可以发送请求的（可以在任务完成的时候通知worker），只是我懒得写了。

血的教训！rpc的字段首字母要大写，在go中只有首字母大写的变量或方法才能导出，在其他包使用，类似于首字母大写是public，小写是private。worker和coordinator虽然在同一个包mr中，但它们之间的通信是用rpc的，似乎也是当作不同的包自己的通信。我刚开始在rpc的request和response中有的变量首字母没有大写，导致我coordinator在reply中将字段设好了，但是worker接收到的一直是0。debug了好久。

在main的mrworker中，先是使用loadPlugin得到map和reduce函数，只有传给mr.Worker(mapf, reducef)，这样就创建好了worker线程。所以worker线程就是一直执行Worker这个函数。Worker函数要一直向coordinator请求任务，执行任务，执行完成后通知coordinator，然后继续请求任务，直到所有的任务都完成，就结束。

在main的mrcoordinator中调用mr.MakeCoordinator初始化coordinator，在c.server()中执行go http.Serve(l, nil)，这样coordinator就会一直监听接收来自worker的请求并处理，直到m.Done()为true，任务完成，监听才会结束。

### mapreduce

worker调用CallForJob()请求任务，如果任务都完成了就return，如果目前没有任务就sleep一段时间之后继续CallForJob()。有任务的话，在CallForJob()返回的reply中有要执行的任务的相关信息，包括任务类型，任务编号，要处理的文件等，map和reduce有公用的rpc字段，也有各自的rpc字段，为了方便，只设计了一个rpc的结构体，没用到的字段不用管就行。完成任务后worker会调用CallForFin()告诉coordinator，coordinator会记录目前任务的进度信息。

map阶段要实现的是将输入的文件交给worker处理，处理后得到一系列键值对（要存储到临时的中间文件中），参考mrsequential适当修改就行。

与mrsequential的区别是，一个worker只需处理一个输入文件，但是需要将处理后得到的键值对依据hash存储到nReduce个中间文件中。具体参考实验指导中的如何将键值对读写到文件即可。

```go
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
```

在reduce中与mrsequential的区别主要是要从多个中间文件中读取得到键值对，后续处理过程和mrsequential差不多。

```go
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
```

### timeout

worker线程可能会失联或者宕机，或者就是单纯的执行的特别慢，类似木桶效应，这样尾延迟就会很大，导致整体性能变差。因此，为了提高整体的性能，可能会让执行速度比较快的worker多执行几个任务。具体地，我设置了一个超时处理函数，如果一个worker执行一个任务过了足够的时间（实验指导中建议设置的10s）还没有完成，就把这个任务重新分配，即重置对应任务的state字段为0，这样在AllocateJob中就会重新分配这个任务。
