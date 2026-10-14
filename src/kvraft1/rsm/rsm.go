package rsm

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	ID 	int64
	Req any
}

type OpResult struct {
	Err 	rpc.Err
	Value 	any
	ID 		int64
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	pending		 map[int]chan OpResult
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:	  make(map[int]chan OpResult),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}


// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.
	id := int64(rand.Int() + rsm.me)

	op := Op{ID:id, Req: req}

	index, _, isLeader := rsm.rf.Start(op)

	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	ch := make(chan OpResult, 1)
	rsm.pending[index] = ch
	rsm.mu.Unlock()
	defer func() {
		rsm.mu.Lock()
		delete(rsm.pending, index)
		rsm.mu.Unlock()
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case res := <-ch:
		if res.ID == op.ID {
			return rpc.OK, res.Value
		} else {
			return rpc.ErrWrongLeader, nil
		}
	case <-timer.C:
		return rpc.ErrWrongLeader, nil
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid == true {
			op := msg.Command.(Op)
			res := rsm.sm.DoOp(op.Req)

			rsm.mu.Lock()
			ch, ok := rsm.pending[msg.CommandIndex]
			if ok {
				result := OpResult{Err: rpc.OK, Value: res, ID:op.ID}
				ch <- result
			}
			rsm.mu.Unlock()
		} else if msg.SnapshotValid == true {

		}
	}
}