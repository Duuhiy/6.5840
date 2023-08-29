package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	FileName string
	TaskId   int
	//TaskStatus int
	TaskType  int
	NReduce   int
	NMap      int
	StartTime time.Time
}

type GetTaskArgs struct {
	WorkerId int
}

type GetTaskResp struct {
	Task Task
}

type RegisterWorkerArgs struct {
}

type RegisterWorkerResp struct {
	Id int
}

type ReportTaskArgs struct {
	Task     Task
	Done     bool
	Err      error
	WorkerId int
}

type ReportTaskResp struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
