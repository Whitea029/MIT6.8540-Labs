package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	mapFinished bool
	allFinished bool
	files       []string
	nextTaskID  int
	mu          sync.Mutex
}

type Task struct {
	FileName  string
	Status    string
	StartTime time.Time
	TaskID    int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// check timeout task
	c.checkTimeout()

	// check if all tasks are finished
	if c.allFinished {
		reply.TaskType = ExitTask
		return nil
	}

	// if map tasks are not finished, assign map tasks
	if !c.mapFinished {
		for i, t := range c.mapTasks {
			if t.Status == Idle {
				reply.TaskType = MapTask
				reply.TaskID = t.TaskID
				reply.FileName = t.FileName
				reply.NReduce = c.nReduce

				// update task status
				c.mapTasks[i].Status = InProgress
				c.mapTasks[i].StartTime = time.Now()
				return nil
			}
		}
		// if no map tasks are idle and not all map tasks are finished
		reply.TaskType = WaitTask
		return nil
	}
	// if map tasks are finished, assign reduce tasks
	for i, t := range c.reduceTasks {
		if t.Status == Idle {
			reply.TaskType = ReduceTask
			reply.TaskID = t.TaskID
			reply.ReduceTaskNum = i
			reply.MapTaskNum = len(c.mapTasks)

			// update task status
			c.reduceTasks[i].Status = InProgress
			c.reduceTasks[i].StartTime = time.Now()
			return nil
		}
	}
	reply.TaskType = WaitTask
	return nil
}

func (c *Coordinator) checkTimeout() {
	timeout := 10 * time.Second
	// check map tasks timeout
	if !c.mapFinished {
		allCompleted := true
		for i, t := range c.mapTasks {
			if t.Status == InProgress && time.Since(t.StartTime) > timeout {
				// if task is timeout, set status to idle
				c.mapTasks[i].Status = Idle
			}
			if t.Status != Completed {
				allCompleted = false
			}
		}
		c.allFinished = allCompleted
	}

	// check reduce tasks timeout
	allCompleted := true
	for i, t := range c.reduceTasks {
		if t.Status == InProgress && time.Since(t.StartTime) > timeout {
			// if task is timeout, set status to idle
			c.reduceTasks[i].Status = Idle
		}
		if t.Status != Completed {
			allCompleted = false
		}
	}
	c.allFinished = allCompleted
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		for i, t := range c.mapTasks {
			if t.TaskID == args.TaskID && t.Status == InProgress {
				c.mapTasks[i].Status = Completed

				// check if all map tasks are finished
				mapFinished := true
				for _, t := range c.mapTasks {
					if t.Status != Completed {
						mapFinished = false
						break
					}
				}
				c.mapFinished = mapFinished
				reply.OK = true
				return nil
			}
		}
	} else if args.TaskType == ReduceTask {
		for i, t := range c.reduceTasks {
			if t.TaskID == args.TaskID && t.Status == InProgress {
				c.reduceTasks[i].Status = Completed

				// check if all reduce tasks are finished
				allFinished := true
				for _, t := range c.reduceTasks {
					if t.Status != Completed {
						allFinished = false
						break
					}
				}
				c.allFinished = allFinished
				reply.OK = true
				return nil
			}
		}
	}
	reply.OK = false
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
		nextTaskID:  0,
		mapFinished: false,
		allFinished: false,
	}

	for i, f := range c.files {
		c.mapTasks[i] = Task{
			FileName: f,
			Status:   Idle,
			TaskID:   i,
		}
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{
			Status: Idle,
			TaskID: i,
		}
	}

	c.server()
	return &c
}
