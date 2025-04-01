package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
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
	WaitTime := 0
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
				//fmt.Println("All tasks are in progress, please wait...")
				time.Sleep(time.Second)
				WaitTime++
				if WaitTime > 5 {
					alive = false
				}
			}
		case Reducetask:
			{
				kva := shuffle(task)
				reduceF(kva, reducef, task)
				CallReduceTaskDone(task)

			}
		case Exittask:
			{
				//fmt.Println("All tasks are in progress, Worker exit")
				alive = false
			}
		}
	}
}

func CallReduceTaskDone(task *Task) {
	args := task
	args.TaskStatus = Finish
	reply := Task{}
	ok := call("Coordinator.ReduceDone", args, &reply)
	if ok {
		//fmt.Println("Task Done!")
	}

}

func CallForTask() *Task {
	args := GetTaskArgs{}
	reply := Task{}
	ok := call("Coordinator.TaskAssignment", &args, &reply)

	if ok {
		// reply.Y should be 100.
		//fmt.Printf("reply.Taskname %v\n", reply.Filename)
	} else {
		//fmt.Printf("call failed!\n")
	}
	return &reply
}
func CallMapTaskDone(task *Task) {
	args := task
	args.TaskStatus = Finish
	reply := Task{}
	ok := call("Coordinator.MapDone", args, &reply)
	if ok {
		//fmt.Println("Task Done!")
	}
}
func mapF(t *Task, mapf func(string, string) []KeyValue) {
	filename := t.Filename[0]
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
	//for _, kv := range intermediate {
	//	//fmt.Printf("%v %v\n", kv.Key, kv.Value)
	//}

	HashKv := make([][]KeyValue, t.ReduceNum)
	for _, v := range intermediate {
		index := ihash(v.Key) % t.ReduceNum
		HashKv[index] = append(HashKv[index], v) // 将该kv键值对放入对应的下标
	}
	for i := 0; i < t.ReduceNum; i++ {
		// 创建唯一的临时文件
		tmpFile, err := os.CreateTemp("", "mr-tmp-"+strconv.Itoa(t.Taskname)+"-"+strconv.Itoa(i)+"-*")
		if err != nil {
			log.Fatal("create temp file failed:", err)
		}

		// 确保如果发生错误，删除临时文件
		defer os.Remove(tmpFile.Name())

		enc := json.NewEncoder(tmpFile)
		for _, kv := range HashKv[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal("encode failed:", err)
			}
		}
		tmpFile.Close()

		// 目标文件
		finalFilename := "mr-tmp-" + strconv.Itoa(t.Taskname) + "-" + strconv.Itoa(i)

		// 原子替换，防止并发写入冲突
		err = os.Rename(tmpFile.Name(), finalFilename)
		if err != nil {
			log.Println("rename failed, cleaning up:", err)
			os.Remove(tmpFile.Name()) // 再次确保失败时删除临时文件
		}
	}
}
func shuffle(t *Task) []KeyValue {
	files := t.Filename
	kva := []KeyValue{}
	//这里给出的是类似mr-tmp-X-{reduce ID}的文件
	for _, fi := range files {
		file, err := os.Open(fi)
		if err != nil {
			log.Fatalf("cannot open %v", fi)
		}
		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}
func reduceF(kva []KeyValue, reducef func(string, []string) string, t *Task) {
	ofile, err := os.CreateTemp("", "mr-out-"+strconv.Itoa(t.Taskname)+"-*")
	if err != nil {
		log.Fatal("create temp file failed:", err)
	}
	defer os.Remove(ofile.Name())
	defer ofile.Close()
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	finalFilename := "mr-out-" + strconv.Itoa(t.Taskname)

	// 原子替换，防止并发写入冲突
	err = os.Rename(ofile.Name(), finalFilename)
	if err != nil {
		log.Println("rename failed, cleaning up:", err)
		os.Remove(ofile.Name()) // 再次确保失败时删除临时文件
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
