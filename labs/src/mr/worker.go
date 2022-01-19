package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		resp := callGetTask()
		switch resp.TaskType {
		case TaskType_Map:
			//fmt.Printf("Do map %d\n", resp.Task.TaskId)
			handleMapTask(resp.Task, mapf)
		case TaskType_Reduce:
			//fmt.Printf("Do reduce %d\n", resp.Task.TaskId)
			handleReduceTask(resp.Task, reducef)
		case TaskType_Wait:
			time.Sleep(time.Second)
		case TaskType_Exit:
			return
		}
	}
}

func handleMapTask(task Task, mapf func(string, string) []KeyValue) {
	fileName := task.MapTask.FileName
	nReduce := task.MapTask.NReduce
	inputFile, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	inputFile.Close()
	kva := mapf(fileName, string(content))
	buffer := map[int][]KeyValue{}
	for _, keyValue := range kva {
		reduceTaskId := ihash(keyValue.Key) % nReduce
		buffer[reduceTaskId] = append(buffer[reduceTaskId], keyValue)
	}
	for hash, kvs := range buffer {
		fileFmt := "mr-%d-%d"
		outFileName := fmt.Sprintf(fileFmt, task.TaskId, hash)
		tempFile, err := ioutil.TempFile("", "mr-map-*")
		if err != nil {
			log.Fatalf("cannot create %v", outFileName)
		}
		encoder := json.NewEncoder(tempFile)
		if err = encoder.Encode(kvs); err != nil {
			log.Fatalf("cannot write %v", outFileName)
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), outFileName)
	}
	callDoneTask(task.TaskId, TaskType_Map)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func handleReduceTask(task Task, reducef func(string, []string) string) {
	nMap := task.ReduceTask.NMap
	var intermediate []KeyValue
	for i := 0; i < nMap; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			continue
			// log.Fatalf("cannot open %v", fileName)
		}
		decoder := json.NewDecoder(file)
		var kvs []KeyValue
		if err := decoder.Decode(&kvs); err != nil {
			log.Fatalf("cannot decode %v", fileName)
		}
		file.Close()
		intermediate = append(intermediate, kvs...)
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	tempFile, err := ioutil.TempFile("", "mr-reduce-*")
	if err != nil {
		log.Fatalf("cannot create %v", tempFile)
	}
	// from mrsequential.go
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	os.Rename(tempFile.Name(), oname)
	callDoneTask(task.TaskId, TaskType_Reduce)
}

func callGetTask() *GetTaskResp {
	req := &GetTaskReq{}
	resp := &GetTaskResp{}
	call("Coordinator.GetTask", req, resp)
	return resp
}

func callDoneTask(taskId int, taskType TaskType) {
	req := &DoneTaskReq{
		TaskType: taskType,
		TaskId:   taskId,
	}
	resp := &DoneTaskResp{}
	call("Coordinator.DoneTask", req, resp)
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
