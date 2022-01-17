package mr

import "fmt"
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	resp := GetJob()
	if !resp.success {
		return
	}
	task := resp.task
	// just do it!
	if task.taskType == TaskType_Map {
		mapWork(task.mapTask.file, mapf)
	} else {
		reduceWork(task.taskId, reducef)
	}
	//TODO tell master I'm done

}

func mapWork(file string, mapf func(string, string) []KeyValue) {

}

func reduceWork(taskId int64, reducef func(string, []string) string) {

}

// GetJob Every one needs a job
// Not to mention a worker!
func GetJob() *GetJobResponse {
	req := GetJobRequest{}
	resp := GetJobResponse{}
	call("Coordinator.GetJob", &req, &resp)
	return &resp
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
