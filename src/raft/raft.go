package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
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
// as each Raft peer becomes aware that successive Log entries are
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

type StartReply struct {
	Index    int
	Term     int
	IsLeader bool
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
		Initialized bool
		CurrentTerm int
		VotedFor    int
		Log         []LogEntry
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
	nextDec []int

	// RPC Channels to avoid race condition
	logRequest   chan AppendEntriesArgs
	voteRequest  chan RequestVoteArgs
	logReply     chan AppendEntriesReply
	voteReply    chan RequestVoteReply
	startRequest chan interface{}
	startReply   chan StartReply

	// Output
	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isleader bool) {
	term = rf.persistent.CurrentTerm
	isleader = rf.nodeType == 0
	return
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persistent.Initialized = true
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
	if !rf.persistent.Initialized {
		DPrintf("0: %d uninitialized persistent status\n", rf.me)
		rf.persistent.CurrentTerm = 0
		rf.persistent.VotedFor = -1
		rf.persistent.Log = make([]LogEntry, 1)
	} else {
		DPrintf("%d: %d Initialized persistent status %v\n", rf.persistent.CurrentTerm, rf.me, rf.persistent)
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

type AppendEntriesReplyToLeader struct {
	Peer             int
	ExpectMatchIndex int
	Reply            AppendEntriesReply
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
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.startRequest <- command
	reply := <-rf.startReply

	return reply.Index, reply.Term, reply.IsLeader
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
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	DPrintf("Initializing %d\n", rf.me)
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
	rf.voteReply = make(chan RequestVoteReply)
	rf.logRequest = make(chan AppendEntriesArgs)
	rf.logReply = make(chan AppendEntriesReply)
	rf.startRequest = make(chan interface{})
	rf.startReply = make(chan StartReply)

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

	leaderReplies := make(chan AppendEntriesReplyToLeader)
	leaderStopRPC := make(chan struct{})
	defer close(leaderStopRPC)

	// send initial heartbeat
	rf.sendHeartbeat(leaderReplies, leaderStopRPC)

	for {
		select {
		case <-ticker.C:
			rf.sendHeartbeat(leaderReplies, leaderStopRPC)
			rf.sendNewAppendEntries(leaderReplies, leaderStopRPC)
		case logReq := <-rf.logRequest:
			rf.logReply <- rf.handleLog(logReq)
		case voteReq := <-rf.voteRequest:
			rf.voteReply <- rf.handleVote(voteReq)
		case startReq := <-rf.startRequest:
			rf.startReply <- rf.handleStart(startReq)
		case logReply := <-leaderReplies:
			rf.handleLogReply(logReply)
		}
		if rf.nodeType != 0 {
			// no longer a leader
			return
		}
		rf.doCommit()
	}
}

func (rf *Raft) candidate() {
	DPrintf("%d: %d started an election\n", rf.persistent.CurrentTerm, rf.me)
	electionTimeout := time.Duration(150*(1+rand.Float32())) * time.Millisecond
	timer := time.After(electionTimeout)
	// start a election
	voteCount := 1
	voteGranted := make([]bool, len(rf.peers))
	voteGranted[rf.me] = true
	voteChan, rejectChan, stopElection := rf.startElection()
	defer close(stopElection)
	for {
		select {
		case i := <-voteChan:
			DPrintf("%d: %d received vote from %d\n", rf.persistent.CurrentTerm, rf.me, i)
			if !voteGranted[i] {
				voteCount++
			}
			voteGranted[i] = true
			if voteCount*2 > len(rf.peers) {
				// We received majority of vote, become a leader
				DPrintf("%d: %d starts to be a leader\n", rf.persistent.CurrentTerm, rf.me)
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				rf.nextDec = make([]int, len(rf.peers))
				for j := range rf.nextIndex {
					rf.nextIndex[j] = len(rf.persistent.Log)
					rf.nextDec[j] = 1
				}
				rf.nodeType = 0
			}
		case newTerm := <-rejectChan:
			if newTerm > rf.persistent.CurrentTerm {
				rf.nodeType = 2
				rf.persistent.CurrentTerm = newTerm
			}
		case <-timer:
			// Restart the election
			return
		case logReq := <-rf.logRequest:
			rf.logReply <- rf.handleLog(logReq)
		case voteReq := <-rf.voteRequest:
			rf.voteReply <- rf.handleVote(voteReq)
		case startReq := <-rf.startRequest:
			rf.startReply <- rf.handleStart(startReq)
		}
		if rf.nodeType != 1 {
			// no longer a candidate
			return
		}
	}
}

func (rf *Raft) follower() {
	DPrintf("%d: %d starts to be a follower\n", rf.persistent.CurrentTerm, rf.me)
	electionTimeout := time.Duration(150*(1+rand.Float32())) * time.Millisecond
	timer := time.After(electionTimeout)
	for {
		select {
		case <-timer:
			// Election timeout, become a candidate
			DPrintf("%d: %d starts to be a candidate\n", rf.persistent.CurrentTerm, rf.me)
			rf.nodeType = 1
		case logReq := <-rf.logRequest:
			timer = time.After(electionTimeout)
			rf.logReply <- rf.handleLog(logReq)
		case voteReq := <-rf.voteRequest:
			rf.voteReply <- rf.handleVote(voteReq)
		case startReq := <-rf.startRequest:
			rf.startReply <- rf.handleStart(startReq)
		}
		if rf.nodeType != 2 {
			// no longer a follower
			return
		}
	}
}

func (rf *Raft) startElection() (voteChan chan int, rejectChan chan int, stopElection chan struct{}) {
	rf.persistent.CurrentTerm++
	rf.persistent.VotedFor = rf.me
	voteChan = make(chan int)
	rejectChan = make(chan int)
	stopElection = make(chan struct{})
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.persist()
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
			rf.persistent.CurrentTerm,
			rf.me,
			len(rf.persistent.Log) - 1,
			rf.persistent.Log[len(rf.persistent.Log)-1].Term,
		})
	}
	return
}

func (rf *Raft) sendHeartbeat(leaderReplies chan AppendEntriesReplyToLeader, leaderStopRPC chan struct{}) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.persist()
		go func(i int, args AppendEntriesArgs) {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(i, args, &reply)
			select {
			case leaderReplies <- AppendEntriesReplyToLeader{i, args.PrevLogIndex, reply}:
			case <-leaderStopRPC:
			}
		}(i, AppendEntriesArgs{
			rf.persistent.CurrentTerm,
			rf.me,
			len(rf.persistent.Log) - 1,
			rf.persistent.Log[len(rf.persistent.Log)-1].Term,
			make([]LogEntry, 0),
			rf.commitIndex,
		})
	}
}

func (rf *Raft) sendNewAppendEntries(leaderReplies chan AppendEntriesReplyToLeader, leaderStopRPC chan struct{}) {
	lastLogIndex := len(rf.persistent.Log) - 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.persist()
		if lastLogIndex >= rf.nextIndex[i] {
			go func(i int, args AppendEntriesArgs) {
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, args, &reply)
				select {
				case leaderReplies <- AppendEntriesReplyToLeader{i, args.PrevLogIndex + len(args.Entries), reply}:
				case <-leaderStopRPC:
				}
			}(i, AppendEntriesArgs{
				rf.persistent.CurrentTerm,
				rf.me,
				rf.nextIndex[i] - 1,
				rf.persistent.Log[rf.nextIndex[i]-1].Term,
				rf.persistent.Log[rf.nextIndex[i]:],
				rf.commitIndex,
			})
		}
	}
}

func (rf *Raft) handleVote(args RequestVoteArgs) (reply RequestVoteReply) {
	// Persist data before RPC return
	defer rf.persist()

	if args.Term > rf.persistent.CurrentTerm {
		// start a new term
		rf.persistent.CurrentTerm = args.Term
		rf.persistent.VotedFor = -1
		rf.nodeType = 2
	}
	reply.Term = rf.persistent.CurrentTerm
	if args.Term < rf.persistent.CurrentTerm {
		DPrintf("%d: %d rejected to vote for %d because that peer has term %d\n", rf.persistent.CurrentTerm, rf.me, args.CandidateID, args.Term)
		reply.VoteGranted = false
		return
	} else if rf.persistent.VotedFor != -1 && rf.persistent.VotedFor != args.CandidateID {
		DPrintf("%d: %d rejected to vote for %d because of it has voted for %d disagree\n", rf.persistent.CurrentTerm, rf.me, args.CandidateID, rf.persistent.VotedFor)
		reply.VoteGranted = false
		return
	} else if args.LastLogTerm < rf.persistent.Log[len(rf.persistent.Log)-1].Term{
		// If the logs have last entries with different terms, then the Log with the later term is more up-to-date.
		DPrintf("%d: %d rejected to vote for %d because of a higher last Log term\n", rf.persistent.CurrentTerm, rf.me, args.CandidateID)
		reply.VoteGranted = false
		return
	} else if args.LastLogTerm == rf.persistent.Log[len(rf.persistent.Log)-1].Term && args.LastLogIndex < len(rf.persistent.Log)-1 {
		DPrintf("%d: %d rejected to vote for %d because of a longer Log\n", rf.persistent.CurrentTerm, rf.me, args.CandidateID)
		reply.VoteGranted = false
		return
	}

	rf.persistent.VotedFor = args.CandidateID
	reply.VoteGranted = true
	DPrintf("%d: %d voted for %d\n", rf.persistent.CurrentTerm, rf.me, args.CandidateID)

	return
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func (rf *Raft) handleLog(args AppendEntriesArgs) (reply AppendEntriesReply) {
	// Persist data before RPC return
	defer rf.persist()

	if args.Term > rf.persistent.CurrentTerm {
		// start a new term
		rf.persistent.CurrentTerm = args.Term
		rf.persistent.VotedFor = -1
		rf.nodeType = 2
	}
	reply.Term = rf.persistent.CurrentTerm
	if args.Term < rf.persistent.CurrentTerm {
		DPrintf("%d: %d rejected append entries from leader %d because that peer has term %d\n", rf.persistent.CurrentTerm, rf.me, args.LeaderID, args.Term)
		reply.Success = false
		return
	}
	if len(rf.persistent.Log) <= args.PrevLogIndex || rf.persistent.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("%d: %d rejected append entries from leader %d because prev log mismatch\n", rf.persistent.CurrentTerm, rf.me, args.LeaderID)
		reply.Success = false
		return
	}
	reply.Success = true
	if len(rf.persistent.Log) > args.PrevLogIndex+1 {
		rf.persistent.Log = rf.persistent.Log[:args.PrevLogIndex+1]
	}
	for _, v := range args.Entries {
		rf.persistent.Log = append(rf.persistent.Log, v)
	}
	for args.LeaderCommit > rf.commitIndex {
		rf.commitIndex += 1
		DPrintf("%d: %d commited log[%d] = %v as follower\n", rf.persistent.CurrentTerm, rf.me, rf.commitIndex, rf.persistent.Log[rf.commitIndex].Command)
		rf.applyCh <- ApplyMsg{rf.commitIndex, rf.persistent.Log[rf.commitIndex].Command, false, nil}
	}
	return
}

func (rf *Raft) handleStart(command interface{}) (reply StartReply) {
	reply.Index = len(rf.persistent.Log)
	reply.Term = rf.persistent.CurrentTerm
	reply.IsLeader = rf.nodeType == 0

	if !reply.IsLeader {
		return
	} else {
		DPrintf("%d: %d received start with command %v", reply.Term, rf.me, command)
		rf.persistent.Log = append(rf.persistent.Log, LogEntry{command, rf.persistent.CurrentTerm})
	}

	return
}

func (rf *Raft) handleLogReply(reply AppendEntriesReplyToLeader) {
	if reply.Reply.Term > rf.persistent.CurrentTerm {
		// start a new term
		rf.persistent.CurrentTerm = reply.Reply.Term
		rf.persistent.VotedFor = -1
		rf.nodeType = 2
		return
	}
	if reply.Reply.Success {
		rf.nextDec[reply.Peer] = 1
		rf.nextIndex[reply.Peer] = reply.ExpectMatchIndex + 1
		rf.matchIndex[reply.Peer] = reply.ExpectMatchIndex
	} else {
		if rf.nextIndex[reply.Peer] > 1 {
			rf.nextIndex[reply.Peer] -= min(rf.nextDec[reply.Peer], rf.nextIndex[reply.Peer] - 1)
			rf.nextDec[reply.Peer] *= 2
		}
	}
}

func (rf *Raft) doCommit() {
	for {
		cnt := 1
		for i := range rf.peers {
			if rf.matchIndex[i] > rf.commitIndex {
				cnt++
			}
		}
		if cnt * 2 > len(rf.peers) {
			rf.commitIndex++
			DPrintf("%d: %d commited Log[%d] = %v as leader\n", rf.persistent.CurrentTerm, rf.me, rf.commitIndex, rf.persistent.Log[rf.commitIndex].Command)
			rf.applyCh <- ApplyMsg{rf.commitIndex, rf.persistent.Log[rf.commitIndex].Command, false, nil}
		} else {
			break
		}
	}
}