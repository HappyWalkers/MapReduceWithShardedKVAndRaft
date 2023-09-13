package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		log.Fatalf(err.Error())
	}
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	for {
		taskReply := GetTaskReply()
		switch taskReply.Task.TaskType {
		case MAP_TASK:
			mapTask := taskReply.Task.ParticularTask.(MapTask)
			applyMap(mapf, &mapTask)
		case REDUCE_TASK:
			reduceTask := taskReply.Task.ParticularTask.(ReduceTask)
			applyReduce(reducef, &reduceTask)
		default:
			log.Fatalf("not a valid task")
		}
	}
}

func applyMap(mapf func(string, string) []KeyValue, task *MapTask) {
	content := readFile(task.FileName)
	keyValueSlice := mapf(task.FileName, string(content))
	storeMapResults(task, keyValueSlice)
	NoticeTaskFinish(Task{TaskType: MAP_TASK, ParticularTask: *task})
}

func applyReduce(reducef func(string, []string) string, task *ReduceTask) {
	keyValueSlice := readReduceTaskInputFile(task)
	sort.Sort(ByKey(keyValueSlice))
	outputString := applyReduceOnEachUniqueKey(reducef, keyValueSlice)
	writeStringToOutputFile(fmt.Sprintf("mr-out-%v", task.TaskID), outputString)
	NoticeTaskFinish(Task{TaskType: REDUCE_TASK, ParticularTask: *task})
}

func writeStringToOutputFile(filename string, outputString string) {
	outputFile, _ := os.Create(filename)
	defer func() {
		err := outputFile.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
	}()
	_, err := outputFile.WriteString(outputString)
	if err != nil {
		log.Fatalf(err.Error())
	}
}

func applyReduceOnEachUniqueKey(reducef func(string, []string) string, keyValueSlice []KeyValue) string {
	i := 0
	var outputStringBuilder strings.Builder
	for i < len(keyValueSlice) {
		j := i + 1
		for j < len(keyValueSlice) && keyValueSlice[j].Key == keyValueSlice[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, keyValueSlice[k].Value)
		}
		output := reducef(keyValueSlice[i].Key, values)

		outputStringBuilder.WriteString(fmt.Sprintf("%v %v\n", keyValueSlice[i].Key, output))

		i = j
	}
	return outputStringBuilder.String()
}

func readReduceTaskInputFile(task *ReduceTask) []KeyValue {
	inputFileDirectoryFile, err := os.Open("./")
	if err != nil {
		log.Fatalf(err.Error())
	}
	dirEntrySlice, err := inputFileDirectoryFile.ReadDir(-1)
	var keyValueSlice []KeyValue
	for _, dirEntry := range dirEntrySlice {
		if dirEntry.IsDir() == false &&
			strings.HasPrefix(dirEntry.Name(), "mr-") &&
			strings.HasSuffix(dirEntry.Name(), "-"+strconv.Itoa(task.TaskID)) {
			file, err := os.Open(dirEntry.Name())
			if err != nil {
				log.Fatalf(err.Error())
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				keyValueSlice = append(keyValueSlice, kv)
			}
		}
	}
	return keyValueSlice
}

func storeMapResults(task *MapTask, keyValueSlice []KeyValue) {
	mapResultsFileJsonEncoderPtrSlice := make([]*json.Encoder, 0, task.ReduceTaskNum)
	for reducerID := 0; reducerID < task.ReduceTaskNum; reducerID += 1 {
		mapResultsFileName := fmt.Sprintf("mr-%v-%v", task.TaskID, reducerID)
		mapResultsFilePtr, err := os.Create(mapResultsFileName)
		if err != nil {
			log.Fatalf(err.Error())
		}
		mapResultsFileJsonEncoderPtrSlice = append(mapResultsFileJsonEncoderPtrSlice, json.NewEncoder(mapResultsFilePtr))
	}
	for _, kv := range keyValueSlice {
		jsonEncoderPtr := mapResultsFileJsonEncoderPtrSlice[ihash(kv.Key)%task.ReduceTaskNum]
		err := jsonEncoderPtr.Encode(&kv)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}
}

func readFile(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	err = file.Close()
	if err != nil {
		log.Fatalf("cannot close %v", filename)
	}
	return content
}

func GetTaskReply() *TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return &reply
	} else {
		log.Fatalf("RPC failed")
		return nil
	}
}

func NoticeTaskFinish(task Task) *NoticeTaskFinishReply {
	args := NoticeTaskFinishArgs{Task: task}
	reply := NoticeTaskFinishReply{}
	ok := call("Coordinator.NoticeTaskFinish", &args, &reply)
	if ok {
		return &reply
	} else {
		log.Fatalf("RPC failed")
		return nil
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
	defer func(c *rpc.Client) {
		err := c.Close()
		if err != nil {
			log.Fatalf(err.Error())
		}
	}(c)

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	} else {
		log.Fatalf(err.Error())
		return false
	}
}
