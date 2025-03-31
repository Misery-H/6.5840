package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

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

	alive := true
	for alive {
		task := CallForTask()
		switch task.Tasktype {
		case Maptask:
			{
				mapF(task, mapf) // 执行map任务
				CallMapTaskDone(task)
			}
		case Waittask:
			{
				fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
			}
		case Reducetask:
			{
				//DoReduceTask(&task, reducef)
			}
		case Exittask:
			{
				fmt.Println("All tasks are in progress, Worker exit")
				alive = false
			}
		}
	}
}

func CallForTask() *Task {
	args := GetTaskArgs{}
	reply := Task{}
	ok := call("Coordinator.TaskAssignment", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Taskname %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
	}
	return &reply
}
func CallMapTaskDone(task *Task) {
	args := task
	args.TaskStatus = Finish
	reply := Task{}
	ok := call("Coordinator.MapDone", args, &reply)
	if ok {
		fmt.Println("Task Done!")
	}
}
func mapF(t *Task, mapf func(string, string) []KeyValue) {
	filename := t.Filename
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	//print intermediate
	//sort.Sort(ByKey(intermediate))
	for _, kv := range intermediate {
		fmt.Printf("%v %v\n", kv.Key, kv.Value)
	}

	HashKv := make([][]KeyValue, t.ReduceNum)
	for _, v := range intermediate {
		index := ihash(v.Key) % t.ReduceNum
		HashKv[index] = append(HashKv[index], v) // 将该kv键值对放入对应的下标
	}
	for i := 0; i < t.ReduceNum; i++ {
		filename := "mr-tmp-" + strconv.Itoa(t.Taskname) + "-" + strconv.Itoa(i)
		//不会把同一个任务同时给两个worker
		new_file, err := os.Create(filename)
		if err != nil {
			log.Fatal("create file failed:", err)
		}
		enc := json.NewEncoder(new_file) // 创建一个新的JSON编码器
		for _, kv := range HashKv[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal("encode failed:", err)
			}
		}
		new_file.Close()

	}
}
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
