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
	Filename   []string //为要处理的文件
	Taskname   int      //输出时的格式，例如mr-1
	Tasktype   int      //约定0为map，1为reduce,2为等待
	ReduceNum  int      //reduce的数量
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
	cond         *sync.Cond //用于map通知
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
		if len(c.ReduceChan) == 0 {
			//fmt.Println("in reduce task")
			reply.Tasktype = Waittask
			return nil
		} else {
			current_task := <-c.ReduceChan
			//记录开始时间
			current_task.startTime = time.Now()
			current_task.TaskStatus = InProcess
			*reply = *current_task
			c.ReduceStatus[current_task.Taskname] = current_task

		}
	} else if c.State == Exittask {
		reply.Tasktype = Exittask
		return nil
	}
	return nil
}
func (c *Coordinator) TaskProduce(files []string, nReduce int) {
	for i := 0; i < len(files); i++ {
		t := &Task{
			Filename:   []string{files[i]},
			Taskname:   i,
			Tasktype:   Maptask,
			ReduceNum:  nReduce,
			startTime:  time.Time{},
			TaskStatus: Wait,
		}
		//fmt.Printf("init:%p\n", t)
		c.MapChan <- t
		c.MapStatus[t.Taskname] = t
		//fmt.Println("init map task", i)
	}
	//默认中间文件有len(files) * nReduce个,直接造出Reduce任务
	for i := 0; i < nReduce; i++ {
		ReduceFiles := make([]string, 0)
		for j := 0; j < len(files); j++ {
			ReduceFiles = append(ReduceFiles, fmt.Sprintf("mr-tmp-%d-%d", j, i))
		}
		t := &Task{
			Filename:   ReduceFiles,
			Taskname:   i,
			Tasktype:   Reducetask,
			ReduceNum:  nReduce,
			startTime:  time.Time{},
			TaskStatus: Wait,
		}
		c.ReduceChan <- t
		c.ReduceStatus[t.Taskname] = t
		//fmt.Println("init reduce task", i)
	}

}
func (c *Coordinator) MapDone(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.MapStatus[args.Taskname] = args
	*reply = *args     // 修复值复制问题
	c.cond.Broadcast() // 通知状态变化
	return nil
}
func (c *Coordinator) ReduceDone(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ReduceStatus[args.Taskname] = args
	*reply = *args     // 修复值复制问题
	c.cond.Broadcast() // 通知状态变化
	return nil
}
func (c *Coordinator) heartCheck() {
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		for !c.MapReady {
			<-ticker.C // 定期触发
			c.mu.Lock()

			for _, v := range c.MapStatus {
				if v.TaskStatus == InProcess && !v.startTime.IsZero() && time.Since(v.startTime) > 10*time.Second {
					//fmt.Println("Map task", v.Filename, "timeout, reassigning task")
					// 任务超时，重置状态
					v.TaskStatus = Wait
					v.startTime = time.Time{}
					// 重新放回任务队列
					select {
					case c.MapChan <- v:
					default:
						//fmt.Println("Map task queue full, unable to reassign task immediately")
					}
				}
			}

			c.mu.Unlock()
		}
		for !c.ReduceReady {
			<-ticker.C // 定期触发
			c.mu.Lock()

			for _, v := range c.ReduceStatus {
				if v.TaskStatus == InProcess && !v.startTime.IsZero() && time.Since(v.startTime) > 10*time.Second {
					//fmt.Println("Reduce task", v.Filename, "timeout, reassigning task")
					// 任务超时，重置状态
					v.TaskStatus = Wait
					v.startTime = time.Time{}
					// 重新放回任务队列
					select {
					case c.ReduceChan <- v:
					default:
						//fmt.Println("Reduce task queue full, unable to reassign task immediately")
					}
				}
			}

			c.mu.Unlock()
		}
	}()
}

func (c *Coordinator) checkMapStatus() {
	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for {
			allDone := true
			for _, v := range c.MapStatus {
				if v.TaskStatus != Finish {
					allDone = false
					break
				}
			}
			if allDone {
				c.MapReady = true
				c.State = Reducetask
				return
			}
			c.cond.Wait() // 使用条件变量等待通知
		}
	}()
}
func (c *Coordinator) checkReduceStatus() {
	go func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for {
			allDone := true
			for _, v := range c.ReduceStatus {
				if v.TaskStatus != Finish {
					allDone = false
					break
				}
			}
			if allDone {
				c.ReduceReady = true
				//fmt.Println("Reduce tasks completed")
				c.State = Exittask
				return
			}
			c.cond.Wait() // 使用条件变量等待通知
		}
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
	return c.State == Exittask
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
	c.cond = sync.NewCond(&c.mu)
	c.TaskProduce(files, nReduce)
	c.checkMapStatus()
	c.heartCheck()
	c.checkReduceStatus()
	c.server()

	//c.checkReduceStatus()
	return &c
}
