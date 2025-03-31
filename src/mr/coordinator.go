package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	Maptask    = 0 // Map任务
	Reducetask = 1 // Reduce任务
	Waittask   = 2 // 等待被分配任务
	Exittask   = 3 // 退出任务

	Wait      = 4
	InProcess = 5
	Finish    = 6
)

type Task struct {
	Filename   string //为要处理的文件
	Taskname   int    //输出时的格式，例如mr-1
	Tasktype   int    //约定0为map，1为reduce,2为等待
	ReduceNum  int    //reduce的数量
	startTime  time.Time
	TaskStatus int // 任务状态
}

type Coordinator struct {
	// Your definitions here.
	State        int        // Map Reduce阶段
	MapChan      chan *Task // Map任务channel
	ReduceChan   chan *Task // Reduce任务channel
	ReduceNum    int        // Reduce的数量
	Files        []string   // 文件
	MapStatus    map[int]*Task
	MapReady     bool // 记录map任务完成的数量
	ReduceStatus map[int]*Task
	ReduceReady  bool // 记录reduce任务完成的数量
	mu           sync.Mutex
}

//var (
//	lock sync.Locker
//)

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskAssignment(args *GetTaskArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.State == Maptask {
		if len(c.MapChan) == 0 {
			reply.Tasktype = Waittask
			return nil
		} else {
			current_task := <-c.MapChan
			//记录开始时间
			current_task.startTime = time.Now()
			current_task.TaskStatus = InProcess
			*reply = *current_task
			c.MapStatus[current_task.Taskname] = current_task
			//fmt.Printf("Assign:%p\n", current_task)
		}
	} else if c.State == Reducetask {

	}
	return nil
}
func (c *Coordinator) MapTaskProduce(files []string, nReduce int) {
	for i := 0; i < len(files); i++ {
		t := &Task{
			Filename:   files[i],
			Taskname:   i,
			Tasktype:   Maptask,
			ReduceNum:  nReduce,
			startTime:  time.Time{},
			TaskStatus: Wait,
		}
		//fmt.Printf("init:%p\n", t)
		c.MapChan <- t
		c.MapStatus[t.Taskname] = t
		fmt.Println(files[i])
	}
	//close(c.MapChan)
}
func (c *Coordinator) MapDone(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.MapStatus[args.Taskname] = args
	fmt.Println("Map task", args.Filename, "  done, waiting for reduce task")
	fmt.Printf("Done:%p\n", args)
	reply = args
	return nil

}
func (c *Coordinator) heartCheck() {
	go func() {
		for !c.MapReady {
			time.Sleep(500 * time.Millisecond) // 添加间隔
			c.mu.Lock()
			for k, v := range c.MapStatus {
				start := v.startTime
				//fmt.Printf("heart:%p\n", k)
				if !start.IsZero() && time.Since(start) > 10*time.Second && c.MapStatus[k].TaskStatus == InProcess {
					fmt.Println("Map task", v.Filename, "timeout", !start.IsZero(), "#", time.Since(start), "#", k)
					c.MapStatus[k].TaskStatus = Wait
					c.MapStatus[k].startTime = time.Time{}
					c.MapChan <- c.MapStatus[k]
				}
			}
			c.mu.Unlock()

		}
		return
	}()
}
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
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	//if v, ok := <-ch; !ok {
	//	ret = true
	//} else {
	//	ch <- v
	//}
	// Your code here.

	return ret
}

func (c *Coordinator) checkMapStatus() {
	go func() {
		for {
			time.Sleep(500 * time.Millisecond) // 添加间隔

			c.mu.Lock()
			allDone := true
			for _, v := range c.MapStatus {
				if v.TaskStatus != Finish {
					allDone = false
					print("in")
					break
				}
			}
			if allDone {
				c.MapReady = true
				c.mu.Unlock()
				fmt.Println("Map tasks completed")

				return // 退出协程
			}
			c.mu.Unlock()
		}
	}()
}
func (c *Coordinator) checkReduceStatus() {
	go func() {
		for {
			time.Sleep(500 * time.Millisecond) // 添加间隔
			if c.MapReady {
				c.mu.Lock()
				allDone := true
				for _, v := range c.ReduceStatus {
					fmt.Printf("checkReduceStatus", v)
					if v.TaskStatus != Finish {
						allDone = false
						break
					}
				}
				if allDone {
					c.ReduceReady = true
					c.mu.Unlock()
					fmt.Println("Map tasks completed")

					return // 退出协程
				}
				c.mu.Unlock()
			}

		}
	}()
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:        0,
		MapChan:      make(chan *Task, len(files)),
		ReduceChan:   make(chan *Task, nReduce),
		ReduceNum:    nReduce,
		Files:        files,
		MapStatus:    make(map[int]*Task),
		ReduceStatus: make(map[int]*Task),
		MapReady:     false,
		ReduceReady:  false,
	}
	c.MapTaskProduce(files, nReduce)

	c.server()
	c.checkMapStatus()
	c.heartCheck()
	//c.checkReduceStatus()
	return &c
}
