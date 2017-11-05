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
	"fmt"
	"labrpc"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	STATE_LEADER = iota
	STATE_CANDIDATE
	STATE_FOLLOWER

	HEARTBEAT = 50 * time.Millisecond
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

type Log struct {
	currentTerm int
	op          int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	state         int
	chanApply     chan ApplyMsg
	chanHeartbeat chan bool
	chanLeader    chan bool

	//persist on all servers
	CurrentTerm int
	VotedFor    int
	VoteCount   int
	Logs        []Log

	//volatile state on all servers
	CommitIndex int
	LastApplied int

	//volatile state on leaders
	NextIndex  []int
	MatchIndex []int

	logger *log.Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == STATE_LEADER
}

func (rf *Raft) GetRole() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) SetRole(state int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = state
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	err := e.Encode(rf.CurrentTerm)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	err = e.Encode(rf.VotedFor)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	err = e.Encode(rf.Logs)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	err := d.Decode(&rf.CurrentTerm)
	if err != nil {
		log.Fatal("decode error:", err)
	}
	err = d.Decode(&rf.VotedFor)
	if err != nil {
		log.Fatal("decode error:", err)
	}
	err = d.Decode(&rf.Logs)
	if err != nil {
		log.Fatal("decode error:", err)
	}
}

//used under lock
func (rf *Raft) converToFollower(currentTerm int) {
	rf.state = STATE_FOLLOWER
	rf.CurrentTerm = currentTerm
	rf.VotedFor = -1
	rf.VoteCount = 0
}

//used under lock
func (rf *Raft) getLastIndex() int {
	return len(rf.Logs) - 1
}

//used under lock
func (rf *Raft) getLastTerm() int {
	return rf.Logs[len(rf.Logs)-1].currentTerm
}

//
// RequestVote RPC handler. Server end
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.logger.Printf("raft: %v ,Receive VoteArgs: %v\n", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer rf.persist()
	reply.VoteGranted = false
	if rf.CurrentTerm > args.Term { //stale term number
		reply.Term = rf.CurrentTerm
		return
	}

	if rf.CurrentTerm < args.Term { //larger term number
		rf.converToFollower(args.Term)
	}

	reply.Term = rf.CurrentTerm
	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId { // have not voted
		latestIndex := rf.getLastIndex()
		latestTerm := rf.getLastTerm()
		if (latestTerm < args.LastLogTerm) || (latestTerm == args.LastLogTerm && latestIndex <= args.LastLogIndex) {
			reply.VoteGranted = true
		}
		if reply.VoteGranted == true {
			rf.VotedFor = args.CandidateId
		}
	}
	return
}

// Client End
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
	rf.logger.Printf("raft: %v, Receive RequestVote reply %v\n", rf.me, *reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != STATE_CANDIDATE { // only candidate can win an vote
			return ok
		}
		if args.Term != rf.CurrentTerm {
			return ok
		}
		if reply.Term > rf.CurrentTerm { // larger term number received
			rf.converToFollower(reply.Term)
			//		rf.persist()
		}
		if reply.VoteGranted {
			rf.VoteCount++
			if rf.state == STATE_CANDIDATE && rf.VoteCount > len(rf.peers)/2 {
				rf.chanLeader <- true
			}
		}
	}
	return ok
}

// AppendEntries RPC handler. Server End
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.logger.Printf("raft: %v ,Receive AppendEntriesArgs: %v\n", rf.me, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if rf.CurrentTerm > args.Term { // receive stale term num
		reply.Term = rf.CurrentTerm
		return
	}

	if args.Term > rf.CurrentTerm { // larger term num
		rf.converToFollower(args.Term)
	}

	reply.Term = args.Term
	reply.Success = true
	rf.chanHeartbeat <- true
	return
}

// Client End
func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.logger.Printf("raft: %v, Receive AppendEntries reply %v\n", rf.me, *reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != STATE_LEADER { // only leader can send AppendEntries
			return ok
		}
		if args.Term != rf.CurrentTerm {
			return ok
		}
		if reply.Term > rf.CurrentTerm { // larger term number received
			rf.converToFollower(reply.Term)
			return ok
		}
	}
	return ok
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

func (rf *Raft) broadcastAppendEntries() {
	var args AppendEntriesArgs
	rf.mu.Lock()
	args.Term = rf.CurrentTerm
	args.LeaderId = rf.me
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i != rf.me && rf.GetRole() == STATE_LEADER {
			go func(id int) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(id, args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = rf.getLastTerm()
	args.LastLogIndex = rf.getLastIndex()
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i != rf.me && rf.GetRole() == STATE_CANDIDATE {
			go func(id int) {
				var reply RequestVoteReply
				rf.logger.Printf("In broadcastRequestVote: %v request vote to %v\n", rf.me, id)
				rf.sendRequestVote(id, args, &reply)
			}(i)
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.VoteCount = 1
	rf.mu.Unlock()
	go rf.broadcastRequestVote()
	select {
	case <-time.After(time.Duration(rand.Int63()%234+777) * time.Millisecond):
	case <-rf.chanHeartbeat:
		rf.logger.Printf("raft: %v. In Candidate , receive heart beat, turn to Follower", rf.me)
		rf.SetRole(STATE_FOLLOWER)
	case <-rf.chanLeader:
		rf.SetRole(STATE_LEADER)
	}
}

func RoleStateMachine(rf *Raft) {
	var state int
	for {
		state = rf.GetRole()
		switch state {
		case STATE_FOLLOWER:
			rf.logger.Printf("raft: %v, currentTerm:%v State_Follower\n", rf.me, rf.CurrentTerm)
			select {
			case <-rf.chanHeartbeat:
				rf.logger.Printf("raft: %v, currentTerm:%v State_Follower, HeartBeat\n", rf.me, rf.CurrentTerm)
			case <-time.After(time.Duration(rand.Int63()%234+777) * time.Millisecond):
				rf.SetRole(STATE_CANDIDATE)
				rf.logger.Printf("raft: %v, currentTerm:%v State_Follower, Timeout to Candidate\n", rf.me, rf.CurrentTerm)
			}
		case STATE_LEADER:
			rf.logger.Printf("raft: %v, currentTerm: %v State_Leader\n", rf.me, rf.CurrentTerm)
			rf.broadcastAppendEntries()
			time.Sleep(HEARTBEAT)
		case STATE_CANDIDATE:
			rf.logger.Printf("raft: %v, currentTerm: %v State_Candidate\n", rf.me, rf.CurrentTerm)
			rf.startElection()
		}
	}
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

	// Your initialization code here.
	rf.state = STATE_FOLLOWER
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanLeader = make(chan bool)
	rf.chanApply = applyCh
	rf.Logs = []Log{Log{0, -1}}

	fileName := fmt.Sprintf("log/raft_%v.log", rf.me)
	logFile, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("open file %v error !", fileName)
	}
	rf.logger = log.New(logFile, "[DEBUG]", log.Lshortfile)

	rf.logger.Printf("raft :%v, current term: %v\n", rf.me, rf.CurrentTerm)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go RoleStateMachine(rf)

	return rf
}
