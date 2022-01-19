package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type ReduceTask struct {
	NMap int
}

type MapTask struct {
	FileName string
	NReduce  int
}

type TaskStatus int

var (
	TaskStatus_Idle     TaskStatus = 0
	TaskStatus_Running  TaskStatus = 1
	TaskStatus_Finished TaskStatus = 2
)

type Task struct {
	TaskId     int
	MapTask    MapTask
	ReduceTask ReduceTask
	TaskStatus TaskStatus
}

type TaskPhase int

var (
	TaskPhase_Map    TaskPhase = 0
	TaskPhase_Reduce TaskPhase = 1
)

type Coordinator struct {
	nMap    int
	nReduce int
	phase   TaskPhase
	allDone bool

	taskTimeOut map[int]time.Time
	tasks       []*Task

	getTaskChan   chan GetTaskMsg
	doneTaskChan  chan DoneTaskMsg
	doneCheckChan chan DoneCheckMsg
	timeoutChan   chan TimeoutMsg
}

type GetTaskMsg struct {
	resp *GetTaskResp
	ok   chan struct{}
}

type DoneTaskMsg struct {
	req *DoneTaskReq
	ok  chan struct{}
}

type DoneCheckMsg struct {
	res *bool
	ok  chan struct{}
}

type TimeoutMsg struct {
	ok chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	msg := DoneCheckMsg{
		res: &ret,
		ok:  make(chan struct{}),
	}
	c.doneCheckChan <- msg
	<-msg.ok
	return ret
}

func (c *Coordinator) getTaskHandler(msg GetTaskMsg) {
	resp := msg.resp
	allDone := true
	for _, task := range c.tasks {
		if task.TaskStatus == TaskStatus_Idle {
			// 发现空闲任务
			resp.TaskType = TaskType_Map
			if c.phase == TaskPhase_Reduce {
				resp.TaskType = TaskType_Reduce
			}
			resp.Task = *task
			task.TaskStatus = TaskStatus_Running
			c.taskTimeOut[task.TaskId] = time.Now()
			msg.ok <- struct{}{}
			return
		}
		if task.TaskStatus != TaskStatus_Finished {
			allDone = false
		}
	}
	// 没有空闲任务
	if c.phase == TaskPhase_Map {
		// 仍位于 map 阶段
		resp.TaskType = TaskType_Wait
		msg.ok <- struct{}{}
		return
	} else {
		// 位于 reduce 阶段
		if !allDone {
			// 没有全部结束，等待
			resp.TaskType = TaskType_Wait
			msg.ok <- struct{}{}
		} else {
			// 全部结束，可以退出
			resp.TaskType = TaskType_Exit
			msg.ok <- struct{}{}
		}
	}
}

func (c *Coordinator) doneTaskHandler(msg DoneTaskMsg) {
	req := msg.req
	if req.TaskType == TaskType_Map && c.phase == TaskPhase_Reduce {
		// 提交非当前阶段的任务，直接返回
		msg.ok <- struct{}{}
		return
	}
	for _, task := range c.tasks {
		if task.TaskId == req.TaskId {
			// 无论当前状态，直接改为完成
			task.TaskStatus = TaskStatus_Finished
			break
		}
	}
	// 删除 timeout 结构
	delete(c.taskTimeOut, req.TaskId)
	allDone := true
	for _, task := range c.tasks {
		if task.TaskStatus != TaskStatus_Finished {
			allDone = false
			break
		}
	}
	if allDone {
		if c.phase == TaskPhase_Map {
			c.initReducePhase()
		} else {
			c.allDone = true
		}
	}
	msg.ok <- struct{}{}
}

func (c *Coordinator) timeoutHandler(msg TimeoutMsg) {
	now := time.Now()
	for taskId, start := range c.taskTimeOut {
		if now.Sub(start).Seconds() > 10 {
			for _, task := range c.tasks {
				if taskId == task.TaskId {
					if task.TaskStatus != TaskStatus_Finished {
						task.TaskStatus = TaskStatus_Idle
					}
					break
				}
			}
			delete(c.taskTimeOut, taskId)
			break
		}
	}
	msg.ok <- struct{}{}
	return
}

func (c *Coordinator) doneCheckHandler(msg DoneCheckMsg) {
	*msg.res = c.allDone
	msg.ok <- struct{}{}
}

// 只在这个 goroutine 中操作结构
func (c *Coordinator) schedule() {
	for {
		select {
		case msg := <-c.getTaskChan:
			c.getTaskHandler(msg)
		case msg := <-c.doneTaskChan:
			c.doneTaskHandler(msg)
		case msg := <-c.timeoutChan:
			c.timeoutHandler(msg)
		case msg := <-c.doneCheckChan:
			c.doneCheckHandler(msg)
		}
	}
}

func (c *Coordinator) GetTask(_ *GetTaskReq, resp *GetTaskResp) error {
	msg := GetTaskMsg{
		resp: resp,
		ok:   make(chan struct{}),
	}
	c.getTaskChan <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) DoneTask(req *DoneTaskReq, _ *DoneTaskResp) error {
	msg := DoneTaskMsg{
		req: req,
		ok:  make(chan struct{}),
	}
	c.doneTaskChan <- msg
	<-msg.ok
	return nil
}

// 初始化 map 任务阶段
func (c *Coordinator) initMapPhase(fileNames []string) {
	c.phase = TaskPhase_Map
	for i, fileName := range fileNames {
		c.tasks = append(c.tasks, &Task{
			TaskId: i,
			MapTask: MapTask{
				FileName: fileName,
				NReduce:  c.nReduce,
			},
			TaskStatus: TaskStatus_Idle,
		})
	}
}

func (c *Coordinator) initReducePhase() {
	c.phase = TaskPhase_Reduce
	c.taskTimeOut = map[int]time.Time{}
	c.tasks = nil
	for i := 0; i < c.nReduce; i++ {
		c.tasks = append(c.tasks, &Task{
			TaskId: i,
			ReduceTask: ReduceTask{
				NMap: c.nMap,
			},
			TaskStatus: TaskStatus_Idle,
		})
	}
}

func (c *Coordinator) timeoutCheck() {
	for {
		msg := TimeoutMsg{
			ok: make(chan struct{}),
		}
		c.timeoutChan <- msg
		<-msg.ok
		time.Sleep(10 * time.Second)
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:          len(files),
		nReduce:       nReduce,
		taskTimeOut:   map[int]time.Time{},
		getTaskChan:   make(chan GetTaskMsg),
		doneTaskChan:  make(chan DoneTaskMsg),
		doneCheckChan: make(chan DoneCheckMsg),
		timeoutChan:   make(chan TimeoutMsg),
	}
	c.initMapPhase(files)
	go c.schedule()
	go c.timeoutCheck()
	c.server()
	return &c
}
