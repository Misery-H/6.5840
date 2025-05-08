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
	Filename   []string  // 要处理的文件列表
	Taskname   int       // 输出时的格式，例如 mr-1
	Tasktype   int       // 约定0为map，1为reduce,2为等待
	ReduceNum  int       // reduce 的数量
	startTime  time.Time // 任务开始时间
	TaskStatus int       // 任务状态
}

type Coordinator struct {
	// 状态相关
	State       int  // Map 或 Reduce 阶段
	MapReady    bool // 是否所有 map 任务完成
	ReduceReady bool // 是否所有 reduce 任务完成

	// 任务通道和任务状态记录
	MapChan      chan *Task // Map任务通道
	ReduceChan   chan *Task // Reduce任务通道
	MapStatus    map[int]*Task
	ReduceStatus map[int]*Task

	// 其他信息
	ReduceNum int      // Reduce 任务数量
	Files     []string // 文件列表

	// 同步相关
	mu   sync.Mutex
	cond *sync.Cond // 条件变量，用于等待状态变化
}

// RPC处理函数：任务分配
func (c *Coordinator) TaskAssignment(args *GetTaskArgs, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.State == Maptask {
		if len(c.MapChan) == 0 {
			reply.Tasktype = Waittask
			return nil
		} else {
			current_task := <-c.MapChan
			// 记录任务开始时间，并设置状态为处理中
			current_task.startTime = time.Now()
			current_task.TaskStatus = InProcess
			*reply = *current_task
			c.MapStatus[current_task.Taskname] = current_task
		}
	} else if c.State == Reducetask {
		if len(c.ReduceChan) == 0 {
			reply.Tasktype = Waittask
			return nil
		} else {
			current_task := <-c.ReduceChan
			// 记录任务开始时间，并设置状态为处理中
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

// 初始化任务：产生 map 和 reduce 任务
func (c *Coordinator) TaskProduce(files []string, nReduce int) {
	// 因为该函数在单线程调用阶段执行，所以无需加锁
	for i := 0; i < len(files); i++ {
		t := &Task{
			Filename:   []string{files[i]},
			Taskname:   i,
			Tasktype:   Maptask,
			ReduceNum:  nReduce,
			startTime:  time.Time{},
			TaskStatus: Wait,
		}
		c.MapChan <- t
		c.MapStatus[t.Taskname] = t
	}
	// 直接构造 reduce 任务（中间文件为 len(files)*nReduce 个）
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
	}
}

// RPC处理函数：Map任务完成
func (c *Coordinator) MapDone(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.MapStatus[args.Taskname] = args
	*reply = *args     // 值复制
	c.cond.Broadcast() // 通知等待中的协程
	return nil
}

// RPC处理函数：Reduce任务完成
func (c *Coordinator) ReduceDone(args *Task, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ReduceStatus[args.Taskname] = args
	*reply = *args     // 值复制
	c.cond.Broadcast() // 通知等待中的协程
	return nil
}

// 心跳检测：定期检测任务超时情况并重分配任务
func (c *Coordinator) heartCheck() {
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		// 检查 Map 任务
		for {
			c.mu.Lock()
			if c.MapReady {
				c.mu.Unlock()
				break
			}
			for _, v := range c.MapStatus {
				if v.TaskStatus == InProcess && !v.startTime.IsZero() && time.Since(v.startTime) > 10*time.Second {
					// 超时重置任务状态并重新放入任务队列
					v.TaskStatus = Wait
					v.startTime = time.Time{}
					select {
					case c.MapChan <- v:
					default:
						// 如果任务通道已满，则暂不处理
					}
				}
			}
			c.mu.Unlock()
			<-ticker.C
		}

		// 检查 Reduce 任务
		for {
			c.mu.Lock()
			if c.ReduceReady {
				c.mu.Unlock()
				break
			}
			for _, v := range c.ReduceStatus {
				if v.TaskStatus == InProcess && !v.startTime.IsZero() && time.Since(v.startTime) > 10*time.Second {
					// 超时重置任务状态并重新放入任务队列
					v.TaskStatus = Wait
					v.startTime = time.Time{}
					select {
					case c.ReduceChan <- v:
					default:
						// 如果任务通道已满，则暂不处理
					}
				}
			}
			c.mu.Unlock()
			<-ticker.C
		}
	}()
}

// 检查 Map 任务状态：等待所有 map 任务完成后转为 Reduce 阶段
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
				c.cond.Broadcast() // 通知其他等待的协程
				return
			}
			c.cond.Wait() // 等待状态变化
		}
	}()
}

// 检查 Reduce 任务状态：等待所有 reduce 任务完成后退出任务调度
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
				c.State = Exittask
				c.cond.Broadcast() // 通知等待中的协程
				return
			}
			c.cond.Wait() // 等待状态变化
		}
	}()
}

// 启动 RPC 服务
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done 方法：返回任务是否全部完成
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.State == Exittask
}

// 构造 Coordinator 对象
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		State:        Maptask,
		MapChan:      make(chan *Task, len(files)),
		ReduceChan:   make(chan *Task, nReduce),
		ReduceNum:    nReduce,
		Files:        files,
		MapStatus:    make(map[int]*Task),
		ReduceStatus: make(map[int]*Task),
		MapReady:     false,
		ReduceReady:  false,
	}
	c.mu = sync.Mutex{}
	c.cond = sync.NewCond(&c.mu)
	c.TaskProduce(files, nReduce)
	c.checkMapStatus()
	c.heartCheck()
	c.checkReduceStatus()
	c.server()

	return &c
}
