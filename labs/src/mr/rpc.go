package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type GetJobRequest struct {
}

type GetJobResponse struct {
	Success bool
	Task    Task
}

type ImDoneRequest struct {
	TaskType TaskType
	TaskId   int
}

type ImDoneResponse struct {
}

type MapDoneRequest struct {
}

type MapDoneResponse struct {
	Done bool
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
