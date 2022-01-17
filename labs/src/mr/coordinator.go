package mr

import (
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskType int64

var (
	TaskType_Map    TaskType = 0
	TaskType_Reduce TaskType = 1
)

type MapTask struct {
	file string
}

type ReduceTask struct {
}

type Task struct {
	taskId   int64
	taskType TaskType

	mapTask    *MapTask
	reduceTask *ReduceTask
}

type Coordinator struct {
	idleTask     []*Task
	workingTask  []*Task
	finishedTask []*Task
	lock         sync.Mutex
}

// GetJob get a task for a worker and record the worker state
func (c *Coordinator) GetJob(req *GetJobRequest, resp *GetJobResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.idleTask) == 0 {
		resp.success = false
		return nil
	}
	// pick the first task
	task := c.idleTask[0]
	c.idleTask = c.idleTask[1:]

	resp.success = true
	resp.task = *task

	// add task to working task
	c.workingTask = append(c.workingTask, task)
	return nil
}

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

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	if len(c.idleTask) == 0 && len(c.workingTask) == 0 {
		return true
	}
	return false
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// prepare for tasks
	for index, file := range files {
		c.idleTask = append(c.idleTask, &Task{
			taskId:   int64(index),
			taskType: TaskType_Map,
			mapTask: &MapTask{
				file: file,
			},
		})
	}
	for i := 0; i < nReduce; i++ {
		c.idleTask = append(c.idleTask, &Task{
			taskId:     int64(i),
			taskType:   TaskType_Reduce,
			reduceTask: &ReduceTask{},
		})
	}
	c.server()
	return &c
}
