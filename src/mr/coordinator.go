package mr

import (
	"encoding/gob"
	"log"
	"sync/atomic"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// Coordinator
// Currently, it's assumed that only one map and one reduce job are submitted.
// So coordinator will assign map tasks first. After all the map tasks are finished, the reduce tasks are assigned.
// After all the reduce tasks are finished, the job is done.
type Coordinator struct {
	unassignedTaskChannel chan Task
	runningTaskSet        *MutexMap[Task, struct{}]
	reduceTaskNum         int
	allTasksAreFinished   atomic.Bool
}

// MakeCoordinator creates a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	coordinator := Coordinator{}
	coordinator.unassignedTaskChannel = make(chan Task, max(len(files), nReduce))
	for idx, filename := range files {
		mapTask := MapTask{FileName: filename, TaskID: idx, ReduceTaskNum: nReduce}
		coordinator.unassignedTaskChannel <- Task{TaskType: MAP_TASK, ParticularTask: mapTask}
	}
	coordinator.runningTaskSet = NewMutexMap[Task, struct{}]()
	coordinator.reduceTaskNum = nReduce
	coordinator.allTasksAreFinished.Store(false)

	coordinator.server()
	return &coordinator
}

// TODO: The coordinator can't reliably distinguish between crashed workers,
// workers that are alive but have stalled for some reason,
// and workers that are executing but too slowly to be useful.
// The best you can do is have the coordinator wait for some amount of time,
// and then give up and re-issue the task to a different worker.
// For this lab, have the coordinator wait for ten seconds;
// after that the coordinator should assume the worker has died (of course, it might not have).
func (coordinator *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	task := <-coordinator.unassignedTaskChannel
	reply.Task = task
	coordinator.runningTaskSet.Set(task, struct{}{})
	return nil
}

func (coordinator *Coordinator) NoticeTaskFinish(args *NoticeTaskFinishArgs, reply *NoticeTaskFinishReply) error {
	task := args.Task
	coordinator.runningTaskSet.Delete(task)
	if len(coordinator.unassignedTaskChannel) == 0 && coordinator.runningTaskSet.Len() == 0 {
		if task.TaskType == MAP_TASK {
			for reduceTaskID := 0; reduceTaskID < coordinator.reduceTaskNum; reduceTaskID += 1 {
				reduceTask := ReduceTask{TaskID: reduceTaskID}
				coordinator.unassignedTaskChannel <- Task{TaskType: REDUCE_TASK, ParticularTask: reduceTask}
			}
		} else if task.TaskType == REDUCE_TASK {
			coordinator.allTasksAreFinished.Store(true)
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (coordinator *Coordinator) server() {
	err := rpc.Register(coordinator)
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	if err != nil {
		log.Fatalf(err.Error())
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	err = os.Remove(sockname)
	if err != nil {
		log.Println(err.Error())
	}
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}()
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (coordinator *Coordinator) Done() bool {
	return coordinator.allTasksAreFinished.Load()
}
