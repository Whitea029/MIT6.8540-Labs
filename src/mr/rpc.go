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

const (
	MapTask    = "map"
	ReduceTask = "reduce"
	ExitTask   = "exit"
	WaitTask   = "wait"
)

const (
	Idle       = "idle"
	InProgress = "in-progress"
	Completed  = "completed"
)

type GetTaskArgs struct {
	WorkerID int
}

type GetTaskReply struct {
	// map
	TaskID   int
	TaskType string
	FileName string

	// reduce
	MapTaskNum    int // number of map tasks
	ReduceTaskNum int // index of reduce task
	NReduce       int // number of reduce tasks, also can be unstandend as the size of the reduce task bucket
}

// 汇报任务状态
type ReportTaskArgs struct {
	TaskID    int
	TaskType  string
	WorkerID  int
	Completed bool
}

type ReportTaskReply struct {
	OK bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
