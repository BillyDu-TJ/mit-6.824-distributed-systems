package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
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

var coordSockName string // socket for coordinator


// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// Your worker implementation here.
	for {
		// use RPC to apply task
		reArgs := RequestTasksArgs{}
		reReply := RequestTasksReply{}
		ok := call("Coordinator.AssignTask", &reArgs, &reReply)
		if !ok || (ok && reReply.TaskType == TaskExit) {
			os.Exit(0)
		} 

		switch reReply.TaskType {
		case TaskWait:
			time.Sleep(500 * time.Millisecond)
			continue
		case TaskMap:
			// read files
			content, err := os.ReadFile(reReply.FileName)
			if err != nil {
				log.Fatalf("Cannot read %v", reReply.FileName)
				continue
			}
			kva := mapf(reReply.FileName, string(content))

			// divide into bucket
			nReduceBucket := make([]ByKey, reReply.NReduce)

			for _, kv := range kva {
				index := ihash(kv.Key)
				nReduceBucket[index] = append(nReduceBucket[index], kv)
			}

			// write disk
			for i := 0; i < reReply.NReduce; i++ {
				// write into temperary file
				tempFile, err := os.CreateTemp(".", "mr-temp-*")
				if err != nil {
					log.Fatalf("Cannot create temp file")
				}

				enc := json.NewEncoder(tempFile)
				for _, kv := range nReduceBucket[i] {
					err := enc.Encode(&kv)
					if err != nil {
						log.Fatalf("Cannot encode json %v", err)
					}
				}

				// rename and write disk
				tempFile.Close()

				finalName := fmt.Sprintf("mr-%d-%d", reReply.TaskID, i)
				err = os.Rename(tempFile.Name(), finalName)
				if err != nil {
					log.Fatalf("Cannot rename file %v", err);
				}
			}

		case TaskReduce:
			intermediate := []KeyValue{}
			// read files and get intermediate kv
			for i := 0; i < reReply.NMap; i++ {
				reduceName := fmt.Sprintf("mr-%d-%d", i, reReply.TaskID)
				file, err := os.Open(reduceName)
				if err != nil {
					log.Fatalf("Cannot read %v", reduceName)
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			// Sort intermediate kv
			sort.Sort(ByKey(intermediate))

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
			tempFile, err := os.CreateTemp(".", "mr-temp-out-*")
				if err != nil {
					log.Fatalf("Cannot create temp file")
				}
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
				fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
			}

			// rename and write disk
			tempFile.Close()
			finalName := fmt.Sprintf("mr-out-%d", reReply.TaskID)
			err = os.Rename(tempFile.Name(), finalName)
			if err != nil {
				log.Fatalf("Cannot rename file %v", err);
			}
		}
		// call ReportTask
		repArgs := ReportTasksArgs{reReply.TaskType, reReply.TaskID} 
		repReply := ReportTasksReply{}
		ok = call("coordinator.ReportTask", &repArgs, &repReply)
		if !ok {
			log.Fatalf("Cannot report task to coordinator with task ID %v", reReply.TaskID)
		}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
