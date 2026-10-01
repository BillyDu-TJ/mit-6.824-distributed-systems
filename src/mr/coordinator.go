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

// this describes the global state of
// MapReduce job. belongs to coordinator.
type JobState int 
const (
	PhaseMap = iota
	PhaseReduce
	PhaseDone
	
)

// this describes the state of a single task.
// differs from tasktype, which describes 
// the state of a worker.
type TaskState int
const (
	Idle = iota
	InProgress
	Done
)

type Taskmeta struct {
	state TaskState
	startTime time.Time
}

type Coordinator struct {
	mu sync.Mutex
	jobState JobState
	mapTasks []Taskmeta
	reduceTasks []Taskmeta
	files []string
	nReduce int
	nMap int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *RequestTasksArgs,reply *RequestTasksReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	switch c.jobState {
	case PhaseMap:
		reply.TaskType = TaskMap

		// search for available task
		candidateTaskIndex := -1
		for i,task := range c.mapTasks {
			if task.state == Idle {
				candidateTaskIndex = i
				c.mapTasks[i].startTime = time.Now()
				c.mapTasks[i].state = InProgress

				reply.FileName = c.files[i]
				reply.TaskID = candidateTaskIndex
				return nil
			}
		}

		if candidateTaskIndex == -1 {
			// check if all map tasks are done
			allDone := true
			for _,task := range c.mapTasks {
				if task.state != Done {
					// All map jobs are InProgress, make workers wait
					allDone = false
					reply.TaskType = TaskWait
					return nil
				}
			}
			if allDone == true {
				// All map jobs are Done, change to reduce
				c.jobState = PhaseReduce
				reply.TaskType = TaskWait
				return nil
			}
		}
		
	case PhaseReduce:

	case PhaseDone:
		
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
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
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server(sockname)
	return &c
}
