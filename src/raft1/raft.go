package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// role of every node
type Role int
const (
	Follower Role = iota
	Candidate 
	Leader
)

// frequency of leader's heartbeat
const HeartbeatInterval = 100 * time.Millisecond

// for fail-fast assert function
func (rf *Raft) assert(condition bool, message string) {
	if !condition {
		panic(fmt.Sprintf("[Node %d][Term %d][Role %v] ASSERTION FAILED: %s", 
							rf.me, rf.currentTerm, rf.role, message))
	}
}

// struct of Log
type Log struct {
	Command interface{}
	TermID  int		// the term of leader accepting this job
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        		sync.Mutex          // Lock to protect shared access to this peer's state
	peers     		[]*labrpc.ClientEnd // RPC end points of all peers
	persister 		*tester.Persister   // Object to hold this peer's persisted state
	me        		int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	role 			Role  				// role state of this peer
	currentTerm 	int 			    // last term server has seen
	votedFor    	int 			    // candidateID that receives this peer, null(-1) for no vote
	log				[]Log			    // log entries of raft
	lastHeartbeat 	time.Time  	 	    // last time of getting heartbeat, timestamp for candidate
	electionTimeout time.Duration		// random timeout stamp of this election
	commitIndex		int 				// index of higest enrty has been committed by majority
	lastApplied		int                 // the last log entry index applied by me
	nextIndex		[]int				// the next index leader is going to post, initialized to lastLogIndex + 1
	matchIndex		[]int 				// the highest entry known to be replicated.

	applyCh 		chan raftapi.ApplyMsg // channel to communicate with application
	applyCond		*sync.Cond			  // contition variable for update local state machine
}

func (rf *Raft) setElection() {
	// Election Timeout will be randomly initiate between 300 to 600 ms
	ms := time.Duration(300 + (rand.Int63() % 300))
	rf.electionTimeout = ms * time.Millisecond

	rf.assert(rf.electionTimeout >= 300 * time.Millisecond && rf.electionTimeout <= 600 * time.Millisecond,
			  "election Timeout must between 300 and 600 ms.")
}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeartbeat = time.Now()
	rf.setElection()
}

// becomeFollower transitions the node to Follower for the given term and resets votedFor
func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.assert(rf.role == Follower && rf.votedFor == -1, "State must be Follower with votedFor reset")
}

// becomeCandidate transitions the node to Candidate, increments term, and votes for self
func (rf *Raft) becomeCandidate() {
	rf.assert(rf.role != Leader, "Leader should never become candidate")
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	rf.assert(rf.role == Candidate && rf.votedFor == rf.me, "Candidate must vote for self")
}

// becomeLeader transitions the candidate to Leader
func (rf *Raft) becomeLeader() {
	rf.assert(rf.role == Candidate, "Only candidate can become leader")
	rf.assert(rf.votedFor == rf.me, "Leader must have voted for self")
	rf.role = Leader

	lastLogIndex := len(rf.log) 		// log[0] is dummy entry, so don't have to + 1
	
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex
		rf.matchIndex[i] = 0
	}
}

// checkInvariants verifies fundamental safety properties of the Raft node state
func (rf *Raft) checkInvariants() {
	rf.assert(rf.role == Follower || rf.role == Candidate || rf.role == Leader, "Invalid role state")
	if rf.role == Candidate || rf.role == Leader {
		rf.assert(rf.votedFor == rf.me, "Candidate or Leader must have voted for self")
	}
	rf.assert(rf.votedFor >= -1 && rf.votedFor < len(rf.peers), "votedFor out of bounds")
	rf.assert(len(rf.log) >= 1, "Log base dummy entry missing")
	rf.assert(rf.lastApplied <= rf.commitIndex, "Cannot send uncommited entry to state machine")
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.role == Leader {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}


// field names must start with capital letters!
type RequestVoteArgs struct {
	Term 		 int		// candidate's term
	CandidateID  int		// candidate's index
	LastLogIndex int		// index of candidate's last log entry
	LastLogTerm  int		// term of candidate's last log entry
}

// field names must start with capital letters!
type RequestVoteReply struct {
	Term 		int 		// term of this peer, for candidate update itself
	VoteGranted bool		// true if voted for candidate
}

type AppendEntriesArgs struct {
	Term		 int		// leader's term
	LeaderID 	 int		// leader's ID
	PrevLogIndex int		// index of log entry before the new entry
	PrevLogTerm  int		// term of PrevLogIndex
	Entries 	 []Log      // empty for heartbeat
	LeaderCommit int		// the entries has been committed by leader
}

type AppendEntriesReply struct {
	Term    		int    // current term for leader to update
	Success 	 	bool   // true if follower contain entry matching prevlogindex & prevlogterm
	ConflictTerm 	int    // for fast backup, if reject, the current conflict term
	ConflictIndex   int	   // for fast backup, the first entry index of conflict term
}

// a go routine that wait to update logs between lastApplied and commitIndex.
// using a condition variables to implement
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		var msgs []raftapi.ApplyMsg
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid:	true,
				Command:		rf.log[i].Command,
				CommandIndex:	i,

				// [TODO 3D] snapshot
			})
		}

		rf.lastApplied = rf.commitIndex

		rf.mu.Unlock()

		// send msg to channel
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// default situation
	reply.VoteGranted = false

	// check terms to update our state
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := rf.log[lastLogIndex].TermID
	upToDate := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	// now vote for candidate
	if args.Term == rf.currentTerm && upToDate {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
			rf.votedFor = args.CandidateID
			reply.VoteGranted = true
			rf.resetElectionTimer()
		}
	}

	if reply.VoteGranted {
		rf.assert(rf.votedFor == args.CandidateID && reply.Term == rf.currentTerm && args.Term == rf.currentTerm,
			"vote granted but state mismatch")
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check term, change state
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else if rf.role == Candidate {
		rf.role = Follower
	}

	rf.assert(rf.role == Follower, "Node accepting AppendEntries from a valid leader must be Follower")

	// check prevlog information
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].TermID != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		if args.PrevLogIndex >= len(rf.log) {
			// case 1: this node has no such loog logs
			reply.ConflictTerm = -1
			reply.ConflictIndex = len(rf.log)
		} else {
			// case 2: term id not match
			reply.ConflictTerm = rf.log[args.PrevLogIndex].TermID
			for i := args.PrevLogIndex; i > 0; i-- {
				if rf.log[i].TermID == reply.ConflictTerm - 1 {
					reply.ConflictIndex = i + 1
					break
				}
			}
		}
		return
	}

	// append logs
	for i := 0; i < len(args.Entries); i++ {
		targetIndex := args.PrevLogIndex + 1 + i
		if targetIndex >= len(rf.log) {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		if rf.log[targetIndex].TermID != args.Entries[i].TermID {
			rf.log = append(rf.log[:targetIndex], args.Entries[i:]...)
			break
		}
	}

	// apply leaderCommit logs into state machine
	if args.LeaderCommit > rf.commitIndex {
		newCommit := min(args.LeaderCommit, len(rf.log) - 1)
		rf.commitIndex = newCommit
		// the follower update its machine
		rf.applyCond.Broadcast()
	}

	// reset timer
	rf.resetElectionTimer()

	reply.Success = true
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.becomeCandidate()

	votes := 1 // already vote for myself
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log) - 1].TermID,
	}

	// send election request to peers
	for peerIndex := 0; peerIndex < len(rf.peers); peerIndex++ {
		if peerIndex == rf.me {
			continue
		}

		go func(target int, RVargs RequestVoteArgs) {
			reply := RequestVoteReply{}

			ok := rf.sendRequestVote(target, &RVargs, &reply)
			if !ok {
				return
			}

			// check result
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// ensure state is still valid
			if rf.role != Candidate || rf.currentTerm != RVargs.Term {
				return
			}

			// fallback to Follower
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			// sum up votes
			if reply.VoteGranted {
				votes++

				if votes > len(rf.peers)/2 && rf.role == Candidate {
					rf.becomeLeader()
					go rf.broadcastAppendEntries()
				}
			}
		}(peerIndex, args)
	}
	rf.mu.Unlock()
}

func (rf *Raft) tryAdvanceCommitIndex() {
	matches := make([]int, len(rf.matchIndex))
	copy(matches, rf.matchIndex)
	sort.Ints(matches)

	// In an odd-sized cluster (e.g. 3 or 5), the median is at (len - 1) / 2
	// For example: 3 nodes -> index 1. 5 nodes -> index 2.
	N := matches[(len(matches)-1)/2]
	if N > rf.commitIndex && rf.log[N].TermID == rf.currentTerm {
		rf.commitIndex = N
		// the leader update its machine
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeat = time.Now()

	for peerIndex := 0; peerIndex < len(rf.peers); peerIndex++ {
		if peerIndex == rf.me {
			continue
		}

		prevLogIndex := rf.nextIndex[peerIndex] - 1
		prevLogTerm := rf.log[prevLogIndex].TermID
		entries := append([]Log(nil), rf.log[rf.nextIndex[peerIndex]:]...)

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go func(target int, AEargs AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			ok := rf.sendAppendEntries(target, &AEargs, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.role != Leader || rf.currentTerm != AEargs.Term {
				return
			}

			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}

			if reply.Success {
				rf.matchIndex[target] = AEargs.PrevLogIndex + len(AEargs.Entries)
				rf.nextIndex[target] = rf.matchIndex[target] + 1
				rf.tryAdvanceCommitIndex()
			} else {
				// if follower reject, there are two cases.
				// case 1, follower don't have that loog logs, we can directly
				//         set nextIndex to ConflictIndex
				// case 2, follower do have, but there are two small cases:
				//		   case A: leader have the term of conflict index, we
				//				   will set it to the next index of this term
				//		   case B: leader also don't have this conflict term,
				// 					which means conflict happened more earlier.
				//					so under this situation, we can do the same as case 1
				// Howerver, AppendEntries has idempotent property! it will ignore
				// the entry that already exists. so we can simplified into 1!
				if reply.ConflictIndex < rf.nextIndex[target] {
					rf.nextIndex[target] = max(1, reply.ConflictIndex)
				}
			}
		}(peerIndex, args)
	}

	rf.mu.Unlock()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, -1, false
	}

	entry := Log{
		Command: command,
		TermID:  rf.currentTerm,
	}
	rf.log = append(rf.log, entry)
	index := len(rf.log) - 1
	term := rf.currentTerm
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1

	go rf.broadcastAppendEntries()

	return index, term, true
}

func (rf *Raft) ticker() {
	for true {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		rf.checkInvariants()

		// leader do heartbeat
		if rf.role == Leader && time.Since(rf.lastHeartbeat) > HeartbeatInterval {
			go rf.broadcastAppendEntries()
		}

		// follower timeout, start election
		if rf.role != Leader && time.Since(rf.lastHeartbeat) > rf.electionTimeout {
			go rf.startElection()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 30 and 50
		// milliseconds.
		ms := 30 + (rand.Int63() % 20)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.role = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	rf.log[0] = Log{Command: "Dommy", TermID: -1} // dummy node for log[0], since log entry start with 1
	rf.resetElectionTimer()
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// start applier to update local state machine
	go rf.applier()

	return rf
}
