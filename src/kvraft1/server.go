package kvraft

import (
	"bytes"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type ValueVersion struct {
	Value string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	mu  sync.Mutex
	rsm *rsm.RSM

	// Your definitions here.
	data map[string]*ValueVersion
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch r := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}
		if v, ok := kv.data[r.Key]; ok {
				reply.Value = v.Value
				reply.Version = v.Version
				reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}
		if v, ok := kv.data[r.Key]; ok {
			// key exist in kv server
			if r.Version == v.Version {
				kv.data[r.Key] = &ValueVersion{r.Value, r.Version + 1}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		} else {
			// key not exist
			if r.Version == 0 {
				kv.data[r.Key] = &ValueVersion{r.Value, rpc.Tversion(1)}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrNoKey
			}
		}
		return reply
	default:
		log.Fatalf("unexpected req type %T", req)
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(kv.data); err != nil {
		log.Fatalf("Snapshot encode failed: %v", err)
	}

	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var newdata map[string]*ValueVersion
	if err := d.Decode(&newdata); err != nil {
		log.Fatalf("Snapshot decode faied: %v", err)
	}

	kv.data = newdata
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}
	kv.data = make(map[string]*ValueVersion)


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	labgob.Register(ValueVersion{})
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
