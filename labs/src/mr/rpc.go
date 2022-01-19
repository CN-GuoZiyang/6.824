package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int

var (
	TaskType_Map    TaskType = 0
	TaskType_Reduce TaskType = 1
	TaskType_Wait   TaskType = 2
	TaskType_Exit   TaskType = 3
)

type GetTaskReq struct {
}

type GetTaskResp struct {
	TaskType TaskType
	Task     Task
}

type DoneTaskReq struct {
	TaskType TaskType
	TaskId   int
}

type DoneTaskResp struct {
}

type HeartBeatReq struct {
	TaskType TaskType
	TaskId   int
}

type HeartBeatResp struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
