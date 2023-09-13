package mr

const MAP_TASK = 0
const REDUCE_TASK = 1

type Task struct {
	TaskType       int
	ParticularTask interface{} // refer to MapTask or ReduceTask

}

type MapTask struct {
	FileName      string
	TaskID        int
	ReduceTaskNum int
}

type ReduceTask struct {
	TaskID int
}
