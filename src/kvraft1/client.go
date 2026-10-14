package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)


type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	leader := ck.Leader()
	args := rpc.GetArgs{Key:key}
	reply := rpc.GetReply{}
	for {
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			leader = (leader + 1) % len(ck.servers)
			ck.leader = leader
			time.Sleep(100 * time.Millisecond)
			continue
		} else {
			break
		}
	}
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key:key, Value:value, Version:version} 
	reply := rpc.PutReply{}

	resend := false 
	leader := ck.Leader()
	for {
		ok := ck.clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)
		if !ok || reply.Err == rpc.ErrWrongLeader {
			leader = (leader + 1) % len(ck.servers)
			ck.leader = leader
			resend = true
			time.Sleep(100 * time.Millisecond)
			continue
		} else {
			switch reply.Err {
			case rpc.ErrVersion:
				if !resend {
					return reply.Err
				} else {
					return rpc.ErrMaybe
				}
			default:
				return reply.Err
			}
		}
	}
}
