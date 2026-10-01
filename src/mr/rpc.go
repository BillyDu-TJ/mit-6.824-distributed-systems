package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// declare task types. this describes the state of workers.
type TaskType int;

const (
	TaskMap = iota
	TaskReduce
	TaskWait // special type in map, make workers sleep when no task available.
	TaskExit
)



// below is RPC declarations.

// This struct is used for workers asking for
// new tasks.
type RequestTasksArgs struct {
	
}

type RequestTasksReply struct {
	TaskID int
	TaskType TaskType 
	FileName string
	NReduce int
	NMap int
	
}

// This struct is used for workers reporting
// their status to the coordinator.
type ReportTypeArgs struct {
	TaskType TaskType
	TaskID int
}

type ReportTypeReply struct {
}


