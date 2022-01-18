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

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		resp := GetJob()
		if !resp.Success {
			break
		}
		task := resp.Task
		// just do it!
		var err error
		if task.TaskType == TaskType_Map {
			err = mapWork(task, mapf)
		} else {
			err = reduceWork(task, reducef)
		}
		if err != nil {
			break
		}
		// tell master I'm done
		ImDone(&task)
	}
}

func mapWork(task Task, mapf func(string, string) []KeyValue) error {
	fileName := task.MapTask.File
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	kva := mapf(fileName, string(content))
	buffer := map[int][]KeyValue{}
	for _, keyValue := range kva {
		reduceTaskId := ihash(keyValue.Key) % task.MapTask.ReduceTaskNum
		buffer[reduceTaskId] = append(buffer[reduceTaskId], keyValue)
	}
	for hash, kvs := range buffer {
		fileFmt := "mr-%d-%d"
		outFileName := fmt.Sprintf(fileFmt, task.TaskId, hash)
		tempFile, err := ioutil.TempFile("", "mr-map-*")
		if err != nil {
			log.Fatalf("cannot create %v", outFileName)
			return err
		}
		encoder := json.NewEncoder(tempFile)
		if err = encoder.Encode(kvs); err != nil {
			log.Fatalf("cannot write %v", outFileName)
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), outFileName)
	}
	return nil
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func reduceWork(task Task, reducef func(string, []string) string) error {
	for {
		if MapDone() {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	var intermediate []KeyValue
	for i := 0; i < task.ReduceTask.MapTaskNum; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
			return err
		}
		decoder := json.NewDecoder(file)
		var kvs []KeyValue
		if err := decoder.Decode(&kvs); err != nil {
			break
		}
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
	return nil
}

// GetJob Every one needs a job
// Not to mention a worker!
func GetJob() *GetJobResponse {
	req := GetJobRequest{}
	resp := GetJobResponse{}
	call("Coordinator.GetJob", &req, &resp)
	return &resp
}

// ImDone tell master I'm done
// don't need a response!
func ImDone(task *Task) {
	req := ImDoneRequest{
		TaskType: task.TaskType,
		TaskId:   task.TaskId,
	}
	resp := ImDoneResponse{}
	call("Coordinator.DoneWork", &req, &resp)
}

// MapDone ask if all map work have done
func MapDone() bool {
	req := MapDoneRequest{}
	resp := MapDoneResponse{}
	call("Coordinator.MapDone", &req, &resp)
	return resp.Done
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
