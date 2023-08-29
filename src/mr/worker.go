package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//// for sorting by key.
//type ByKey []KeyValue
//// for sorting by key.
//func (a ByKey) Len() int           { return len(a) }
//func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
//func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.id = CallRegister()
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	fmt.Println("注册完成，开始工作")
	w.run()
}

func (w *worker) run() {
	for {
		t := w.CallGetTask()
		w.doTask(t)
	}
}

func CallRegister() int {
	// declare an argument structure.
	args := RegisterWorkerArgs{}
	// fill in the argument(s).
	// declare a reply structure.
	reply := RegisterWorkerResp{}
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Id)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.Id
}

func (w *worker) CallGetTask() Task {

	// declare an argument structure.
	args := GetTaskArgs{}
	// fill in the argument(s).
	args.WorkerId = w.id
	// declare a reply structure.
	reply := GetTaskResp{}
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Task %v\n", reply.Task)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply.Task
}

func (w *worker) doTask(t Task) {
	if t.TaskType == MapStatus {
		w.doMapTask(t)
	} else {
		w.doReduceTask(t)
	}
}

func (w *worker) doMapTask(t Task) {
	// 1.准备mapf的输入，读取file filenam string, content string
	//fmt.Println("filename : ", t.FileName)
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
	}
	// 2.调用mapf
	//fmt.Println("文件录取完毕，调用mapf")
	kvs := w.mapf(t.FileName, string(contents))
	// 3.保存中间结果
	//fmt.Println("调用mapf完毕，开始处理中间结果")
	reduces := make([][]KeyValue, t.NReduce)
	// 3.1将map结果分成若干个reduce
	//sort.Sort(ByKey(kvs))
	for _, kv := range kvs {
		ind := ihash(kv.Key) % t.NReduce
		reduces[ind] = append(reduces[ind], kv)
	}
	// 3.2保存本地
	for ind, reduce := range reduces {
		filename := reduceName(t.TaskId, ind)
		f, err := os.Create(filename)
		if err != nil {
			w.reportTask(t, false, err)
		}
		enc := json.NewEncoder(f)
		for _, kv := range reduce {
			if err := enc.Encode(kv); err != nil {
				w.reportTask(t, false, err)
			}
		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	// 4.报告任务状态
	w.reportTask(t, true, nil)
}

func (w *worker) doReduceTask(t Task) {
	// 1.准备输入 k string, v []string
	// 1.1 读取NMap个map task 产生的第t.TaskId个 reduce任务
	maps := make(map[string][]string)
	for i := 0; i < t.NMap; i++ {
		filename := reduceName(i, t.TaskId)
		f, err := os.Open(filename)
		if err != nil {
			w.reportTask(t, false, err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	// 2. 调用reducef
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	// 3. 写到本地文件中
	// 0600 文件所有者拥有读写权限 组和其他用户没有权限
	if err := ioutil.WriteFile(mergeName(t.TaskId), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}
	w.reportTask(t, true, nil)
}

func (w *worker) reportTask(t Task, done bool, err error) {
	args := ReportTaskArgs{}
	// fill in the argument(s).
	args.Task = t
	args.Done = done
	args.Err = err
	args.WorkerId = w.id
	// declare a reply structure.
	reply := ReportTaskResp{}
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.ReportTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply %v\n", reply)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func reduceName(mapInd, reduceInd int) string {
	return fmt.Sprintf("mr-%d-%d", mapInd, reduceInd)
}

func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
