package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workID := os.Getpid()

	for {
		task := getTask(workID)
		switch task.TaskType {
		case MapTask:
			doMap(task, mapf, workID)
		case ReduceTask:
			doReduce(task, reducef, workID)
		case WaitTask:
			time.Sleep(500 * time.Millisecond)
		case ExitTask:
			return
		}
	}

}

func doReduce(task GetTaskReply, reducef func(string, []string) string, workID int) {
	reduceTaskNum := task.ReduceTaskNum
	mapTaskNum := task.MapTaskNum

	intermediate := []KeyValue{}

	// read the intermediate files wh
	for i := 0; i < mapTaskNum; i++ {
		fileName := fmt.Sprintf("mr-%d-%d", i, reduceTaskNum)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open on reducing %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	log.Printf("intermediatelen: %d", len(intermediate))
	// sort the intermediate key/value pairs
	sort.Sort(ByKey(intermediate))

	// create a file to write the output
	tmp, err := os.CreateTemp("", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("cannot create output file")
	}

	// call reduce for each distinct key in intermediate[],
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
		// write the output to the file
		fmt.Fprintf(tmp, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tmp.Close()

	// atomic rename
	err = os.Rename(tmp.Name(), fmt.Sprintf("mr-out-%d", reduceTaskNum))
	if err != nil {
		log.Fatalf("cannot rename output file")
	}

	// report task done
	reportTaskDone(ReduceTask, task.TaskID, workID)
}

func doMap(task GetTaskReply, mapf func(string, string) []KeyValue, workID int) {
	// read the input file
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open on mapping %v", task.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}
	file.Close()
	// call the user-defined Map function
	kva := mapf(task.FileName, string(content))
	// create intermediate files
	intermediateFiles := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		intermediateFiles[bucket] = append(intermediateFiles[bucket], kv)
	}
	// write the intermediate files
	for i := 0; i < task.NReduce; i++ {
		tmp, err := os.CreateTemp("", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file")
		}
		enc := json.NewEncoder(tmp)
		for _, kv := range intermediateFiles[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("cannot write to temp file")
			}
		}
		tmp.Close()
		// atomic rename
		err = os.Rename(tmp.Name(), fmt.Sprintf("mr-%d-%d", task.TaskID, i))
		if err != nil {
			log.Fatalf("cannot rename temp file")
		}
	}
	reportTaskDone(MapTask, task.TaskID, workID)
}

func reportTaskDone(taskType string, taskID int, workID int) {
	args := ReportTaskArgs{
		TaskID:    taskID,
		TaskType:  taskType,
		WorkerID:  workID,
		Completed: true,
	}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		log.Fatal("call failed!")
	}
	if !reply.OK {
		log.Fatalf("report task failed!")
	}
}

func getTask(workID int) GetTaskReply {
	args := GetTaskArgs{
		WorkerID: workID,
	}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Fatal("call failed!")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
