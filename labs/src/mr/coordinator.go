package mr

import (
	"log"
	"sync"
	"time"
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
	File          string
	ReduceTaskNum int
}

type ReduceTask struct {
	MapTaskNum int
}

type Task struct {
	TaskId   int
	TaskType TaskType

	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type TaskPool struct {
	mapTask    map[int]*Task
	reduceTask map[int]*Task
}

type Coordinator struct {
	idleTask     TaskPool
	workingTask  TaskPool
	finishedTask []*Task
	lock         sync.RWMutex
	taskStartMap map[*Task]time.Time
}

// GetJob get a task for a worker and record the worker state
func (c *Coordinator) GetJob(req *GetJobRequest, resp *GetJobResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if len(c.idleTask.mapTask) == 0 && len(c.idleTask.reduceTask) == 0 {
		resp.Success = false
		return nil
	}
	// pick the first task
	var task *Task
	for taskId, t := range c.idleTask.mapTask {
		task = t
		delete(c.idleTask.mapTask, taskId)
		break
	}
	if task == nil {
		// get reduce task
		for taskId, t := range c.idleTask.reduceTask {
			task = t
			delete(c.idleTask.reduceTask, taskId)
			break
		}
	}
	if task == nil {
		// something wrong! return
		resp.Success = false
		return nil
	}
	resp.Success = true
	resp.Task = *task

	// add task to working task
	if task.TaskType == TaskType_Map {
		c.workingTask.mapTask[task.TaskId] = task
	} else {
		c.workingTask.reduceTask[task.TaskId] = task
	}
	c.taskStartMap[task] = time.Now()

	return nil
}

// DoneWork someone has finished his job!
func (c *Coordinator) DoneWork(req *ImDoneRequest, resp *ImDoneResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	if req.TaskType == TaskType_Map {
		if _, ok := c.workingTask.mapTask[req.TaskId]; !ok {
			// repeated submit
			return nil
		}
		task := c.workingTask.mapTask[req.TaskId]
		delete(c.workingTask.mapTask, req.TaskId)
		delete(c.taskStartMap, task)
		c.finishedTask = append(c.finishedTask, task)
	} else {
		if _, ok := c.workingTask.reduceTask[req.TaskId]; !ok {
			// repeated submit
			return nil
		}
		task := c.workingTask.reduceTask[req.TaskId]
		delete(c.workingTask.reduceTask, req.TaskId)
		delete(c.taskStartMap, task)
		c.finishedTask = append(c.finishedTask, task)
	}
	return nil
}

// MapDone find out if all map work have done
func (c *Coordinator) MapDone(req *MapDoneRequest, resp *MapDoneResponse) error {
	c.lock.RLock()
	defer c.lock.RUnlock()
	resp.Done = false
	if len(c.idleTask.mapTask) == 0 && len(c.workingTask.mapTask) == 0 {
		resp.Done = true
	}
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
	c.lock.RLock()
	defer c.lock.RUnlock()
	if len(c.idleTask.mapTask) == 0 && len(c.idleTask.reduceTask) == 0 &&
		len(c.workingTask.mapTask) == 0 && len(c.workingTask.reduceTask) == 0 {
		// all clear
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
	c.idleTask.mapTask = map[int]*Task{}
	c.idleTask.reduceTask = map[int]*Task{}
	c.workingTask.mapTask = map[int]*Task{}
	c.workingTask.reduceTask = map[int]*Task{}
	c.taskStartMap = map[*Task]time.Time{}
	// prepare for tasks
	for index, file := range files {
		c.idleTask.mapTask[index] = &Task{
			TaskId:   index,
			TaskType: TaskType_Map,
			MapTask: &MapTask{
				File:          file,
				ReduceTaskNum: nReduce,
			},
		}
	}
	for i := 0; i < nReduce; i++ {
		c.idleTask.reduceTask[i] = &Task{
			TaskId:   i,
			TaskType: TaskType_Reduce,
			ReduceTask: &ReduceTask{
				MapTaskNum: len(files),
			},
		}
	}
	go func() {
		for {
			time.Sleep(time.Second)
			now := time.Now()
			c.lock.Lock()
			for task, startTime := range c.taskStartMap {
				if now.Sub(startTime).Seconds() > 10 {
					// timeout!
					if task.TaskType == TaskType_Map {
						delete(c.workingTask.mapTask, task.TaskId)
						c.idleTask.mapTask[task.TaskId] = task
					} else {
						delete(c.workingTask.reduceTask, task.TaskId)
						c.idleTask.reduceTask[task.TaskId] = task
					}
					delete(c.taskStartMap, task)
				}
			}
			c.lock.Unlock()
		}
	}()
	c.server()
	return &c
}
