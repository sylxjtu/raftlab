package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// LogEntry struct
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	persistent struct {
		// Persistent state on all servers
		initialized bool
		currentTerm int
		votedFor    int
		log         []LogEntry
	}

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Auxilary state

	// Leader 0
	// Candidate 1
	// Follower 2
	nodeType int

	logRequest  chan AppendEntriesArgs
	voteRequest chan RequestVoteArgs
	logReply    chan AppendEntriesReply
	voteReply   chan RequestVoteReply
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isleader bool) {
	term = rf.persistent.currentTerm
	isleader = rf.nodeType == 0
	return
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.persistent)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.persistent)
	if !rf.persistent.initialized {
		rf.persistent.currentTerm = 0
		rf.persistent.votedFor = -1
		rf.persistent.log = make([]LogEntry, 1)
	}
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.voteRequest <- args
	*reply = <-rf.voteReply
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs is the RPC argument of AppendEntries
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply is the RPC reply of AppendEntries
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.logRequest <- args
	*reply = <-rf.logReply
	return
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// Volatile state on all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Auxilary state
	// Leader 0
	// Candidate 1
	// Follower 2
	// start as follower
	rf.nodeType = 2
	rf.voteRequest = make(chan RequestVoteArgs)
	rf.logRequest = make(chan AppendEntriesArgs)
	rf.voteReply = make(chan RequestVoteReply)
	rf.logReply = make(chan AppendEntriesReply)

	go func() {
		for {
			switch rf.nodeType {
			// Leader
			case 0:
				rf.leader()
			case 1:
				rf.candidate()
			case 2:
				rf.follower()
			}
		}
	}()

	return rf
}

func (rf *Raft) leader() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	// send initial heartbeat
	rf.sendHeartbeat()

	for {
		select {
		case <-ticker.C:
			rf.sendHeartbeat()
		case logReq := <-rf.logRequest:
			rf.logReply <- rf.handleLog(logReq)
			if rf.nodeType != 0 {
				// no longer a leader
				return
			}
		case voteReq := <-rf.voteRequest:
			rf.voteReply <- rf.handleVote(voteReq)
			if rf.nodeType != 0 {
				// no longer a leader
				return
			}
		}
	}
}

func (rf *Raft) candidate() {
	DPrintf("%d: %d started an election\n", rf.persistent.currentTerm, rf.me)
	electionTimeout := time.Duration(150*(1+rand.Float32())) * time.Millisecond
	// start a election
	voteCount := 1
	voteGranted := make([]bool, len(rf.peers))
	voteGranted[rf.me] = true
	voteChan, rejectChan, stopElection := rf.startElection()
	defer close(stopElection)
	for {
		select {
		case i := <-voteChan:
			DPrintf("%d: %d received vote from %d\n", rf.persistent.currentTerm, rf.me, i)
			if !voteGranted[i] {
				voteCount++
			}
			voteGranted[i] = true
			if voteCount*2 > len(rf.peers) {
				// We received majority of vote, become a leader
				DPrintf("%d: %d starts to be a leader\n", rf.persistent.currentTerm, rf.me)
				rf.nextIndex = make([]int, 0)
				rf.matchIndex = make([]int, 0)
				rf.nodeType = 0
				return
			}
		case newTerm := <-rejectChan:
			rf.nodeType = 2
			rf.persistent.currentTerm = newTerm
			return
		case <-time.After(electionTimeout):
			// Restart the election
			return
		case logReq := <-rf.logRequest:
			rf.logReply <- rf.handleLog(logReq)
			if rf.nodeType != 1 {
				// no longer a candidate
				return
			}
		case voteReq := <-rf.voteRequest:
			rf.voteReply <- rf.handleVote(voteReq)
			if rf.nodeType != 1 {
				// no longer a candidate
				return
			}
		}
	}
}

func (rf *Raft) follower() {
	DPrintf("%d: %d starts to be a follower\n", rf.persistent.currentTerm, rf.me)
	electionTimeout := time.Duration(150*(1+rand.Float32())) * time.Millisecond
	for {
		select {
		case <-time.After(electionTimeout):
			// Election timeout, become a candidate
			DPrintf("%d: %d starts to be a candidate\n", rf.persistent.currentTerm, rf.me)
			rf.nodeType = 1
			return
		case logReq := <-rf.logRequest:
			rf.logReply <- rf.handleLog(logReq)
		case voteReq := <-rf.voteRequest:
			rf.voteReply <- rf.handleVote(voteReq)
		}
	}
}

func (rf *Raft) startElection() (voteChan chan int, rejectChan chan int, stopElection chan interface{}) {
	rf.persistent.currentTerm++
	rf.persistent.votedFor = rf.me
	voteChan = make(chan int)
	rejectChan = make(chan int)
	stopElection = make(chan interface{})
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, args RequestVoteArgs) {
			reply := RequestVoteReply{}
			rpcStatus := rf.sendRequestVote(i, args, &reply)
			if rpcStatus {
				if reply.VoteGranted {
					select {
					case voteChan <- i:
					case <-stopElection:
					}
				} else {
					select {
					case rejectChan <- reply.Term:
					case <-stopElection:
					}
				}
			}
		}(i, RequestVoteArgs{
			rf.persistent.currentTerm,
			rf.me,
			len(rf.persistent.log) - 1,
			rf.persistent.log[len(rf.persistent.log)-1].Term,
		})
	}
	return
}

func (rf *Raft) sendHeartbeat() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(i, args, &reply)
		}(i, AppendEntriesArgs{
			rf.persistent.currentTerm,
			rf.me,
			len(rf.persistent.log) - 1,
			rf.persistent.log[len(rf.persistent.log)-1].Term,
			make([]LogEntry, 0),
			rf.commitIndex,
		})
	}
	return
}

func (rf *Raft) handleVote(args RequestVoteArgs) (reply RequestVoteReply) {
	// Persist data before RPC return
	defer rf.persist()

	reply.Term = rf.persistent.currentTerm
	if args.Term > rf.persistent.currentTerm {
		// start a new term
		rf.persistent.currentTerm = args.Term
		rf.persistent.votedFor = -1
		rf.nodeType = 2
	}

	if args.Term < rf.persistent.currentTerm {
		DPrintf("%d: %d rejected to vote for %d because that peer has term %d\n", rf.persistent.currentTerm, rf.me, args.CandidateID, args.Term)
		reply.VoteGranted = false
	} else if rf.persistent.votedFor == -1 || rf.persistent.votedFor == args.CandidateID {
		DPrintf("%d: %d voted for %d\n", rf.persistent.currentTerm, rf.me, args.CandidateID)
		rf.persistent.votedFor = args.CandidateID
		reply.VoteGranted = true
	}

	if !reply.VoteGranted {
		DPrintf("%d: %d rejected to vote for %d because of it has voted for %d disagree\n", rf.persistent.currentTerm, rf.me, args.CandidateID, rf.persistent.votedFor)
	}
	return
}

func (rf *Raft) handleLog(args AppendEntriesArgs) (reply AppendEntriesReply) {
	// Persist data before RPC return
	defer rf.persist()

	reply.Term = rf.persistent.currentTerm
	if args.Term > rf.persistent.currentTerm {
		// start a new term
		rf.persistent.currentTerm = args.Term
		rf.persistent.votedFor = -1
		rf.nodeType = 2
	}
	if args.Term < rf.persistent.currentTerm {
		DPrintf("%d: %d rejected append entries from leader %d because that peer has term %d\n", rf.persistent.currentTerm, rf.me, args.LeaderID, args.Term)
		reply.Success = false
	} else {
		// DPrintf("%d: %d accepted append entries from leader %d TODO\n", rf.persistent.currentTerm, rf.me, args.LeaderID)
		reply.Success = true
	}
	return
}
