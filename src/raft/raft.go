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
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// ApplyMsg is meassage applied to state machine
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FOLLOWER            = iota //0
	CANDIDATE                  //1
	LEADER                     //2
	TimeoutLowerMS      = 250
	TimeoutUpperMS      = 400
	HeartBeatIntervalMS = 100
	LoopCheckIntervalMS = 50
	RetryRPCIntervalMS  = 50
	enableLog           = false
)

type leader struct {
	nextIndex  []int
	matchIndex []int
}

// LogEntry contain command and the commit term
type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	applyCond *sync.Cond          // to invoke the "applyLoop" thread
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	status    int                 // the status of server

	// persistent state
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// for commit command
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	leaderStatus leader
	votesGot     int
	numNil       int // num of nil command

	// for election timer
	lastTime        time.Time
	timeoutInterval time.Duration
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// Must be wrapped with lock
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state. Must be wrapped with lock
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.printLog("Init without reading from presist")
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		rf.printLog("Init without reading from presist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.printLog("Init from presist")
	}
}

// Update last time of heartbeat. Ensure lock is wrapped
func (rf *Raft) resetTimer() {
	rf.lastTime = time.Now()
	rf.timeoutInterval = (time.Duration(rand.Int31()%(TimeoutUpperMS-TimeoutLowerMS) + TimeoutLowerMS)) * time.Millisecond
}

func (rf *Raft) printLog(a ...interface{}) {
	statusMap := map[int]string{
		FOLLOWER:  "Foll",
		CANDIDATE: "Cand",
		LEADER:    "Lead",
	}

	if enableLog {
		fmt.Print("#", rf.me, " ", statusMap[rf.status], " T", rf.currentTerm, ": ")
		fmt.Println(a...)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// RequestVoteArgs for candidate requests votes
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply for request reply
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// RequestVote candidate request vote from other servers
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.printLog("Reveived higer term in sendRequestVote from server", args.CandidateID, "and become Follower")
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.status == FOLLOWER {
		lastLog := rf.logs[len(rf.logs)-1]
		if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
			reply.VoteGranted = false
			rf.printLog("Reject the vote of server", args.CandidateID, "because I have already voted.")
		} else if lastLog.Term > args.LastLogTerm || (lastLog.Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex) {
			reply.VoteGranted = false
			rf.printLog("Reject the vote of server", args.CandidateID, "because I am more up-to-date.")
		} else {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.persist()
			rf.resetTimer()
			rf.printLog("Grant vote to server", args.CandidateID)
		}
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

// Send request vote and handle the reply. Retry if fails
func (rf *Raft) sendRequestVote(server int, rva *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	// TODO: loop call RequestVote if it fails in this term
	ok := rf.peers[server].Call("Raft.RequestVote", rva, reply)

	if !ok {
		time.Sleep(RetryRPCIntervalMS * time.Millisecond)
		rf.mu.Lock()
		if rva.Term == rf.currentTerm && rf.status == CANDIDATE {
			go rf.sendRequestVote(server, rva)
		}
		rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.printLog("Reveived higer term in sendRequestVote from server", server, ", become Follower")
	}

	if reply.VoteGranted == true {
		if rva.Term == rf.currentTerm && rf.status == CANDIDATE {
			rf.printLog("Get vote from server", server)
			rf.votesGot++
			if rf.votesGot > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		} else {
			rf.printLog("Get vote from server", server, "but is outdated")
		}
	}
	rf.mu.Unlock()

}

// AppendEntriesArgs for heartbeat and appendEntries
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store
	LeaderCommit int        // leader's commitIndex
}

//AppendEntriesReply the reply
type AppendEntriesReply struct {
	Term          int  // current term, for leader to update itself
	Success       bool // true if follower successfully append entries
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries receive appendEntries from leader0
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		rf.printLog("Reveived higer term in AppendEntries from server", args.LeaderID, ", become Follower")
	}

	// same term
	if rf.status == CANDIDATE {
		rf.becomeFollower(args.Term)
		rf.printLog("Receive AppendEntries with the same term. Become follower")
	}

	if args.PrevLogIndex >= len(rf.logs) {
		rf.printLog("args.PrevLogIndex >= len(rf.logs)")
		reply.Success = false
		reply.ConflictIndex = len(rf.logs)
		reply.ConflictTerm = -1
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.logs[args.PrevLogIndex].Term
		for reply.ConflictIndex = args.PrevLogIndex; rf.logs[reply.ConflictIndex-1].Term == reply.ConflictTerm; reply.ConflictIndex-- {
		}
		// conflict, remove this entry and all that follow it
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.persist()
	} else {
		reply.Success = true
		// log match at args.PrevLogIndex, ensure the logs after it match args.Entries
		for i := range args.Entries {
			if args.PrevLogIndex+1+i >= len(rf.logs) {
				rf.logs = append(rf.logs, args.Entries[i])
			} else {
				if args.Entries[i].Term != rf.logs[args.PrevLogIndex+1+i].Term {
					// conflict, remove all that after it
					rf.logs = append(rf.logs[:args.PrevLogIndex+1+i], args.Entries[i])
				}
			}
		}
		rf.persist()
		// rf.logs = append(rf.logs, args.Entries...)
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.applyCond.Broadcast()
	}

	reply.Term = rf.currentTerm
	rf.resetTimer()
	rf.mu.Unlock()
}

// SendAppendEntries send AppendEntries to
func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		// time.Sleep(RetryRPCIntervalMS * time.Millisecond)
		// rf.mu.Lock()
		// if rf.currentTerm == args.Term && rf.status == LEADER && args. {
		// 	rf.issueAppendEntry(server)
		// }
		// rf.mu.Unlock()
		return
	}

	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		rf.printLog("Reveived higer term in sendRequestVote from server", server, ", become Follower")
	}

	if rf.status == LEADER {
		if reply.Success {
			rf.leaderStatus.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.leaderStatus.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1

			// update commit
			indics := make([]int, len(rf.peers))
			copy(indics, rf.leaderStatus.matchIndex)
			indics[rf.me] = len(rf.logs) - 1
			sort.Ints(indics)
			ci := indics[len(rf.peers)/2]

			// 5.4.2 leader can only update commitIndex to current term
			if ci > rf.commitIndex && rf.logs[ci].Term == rf.currentTerm {
				rf.commitIndex = ci
				rf.applyCond.Broadcast()
			}

		} else {
			// append failed, decrement nextIndex and retry
			if reply.ConflictTerm >= 0 {
				index := -1
				for i, log := range rf.logs {
					if log.Term == reply.ConflictTerm {
						index = i
					}
				}
				if index != -1 {
					rf.leaderStatus.nextIndex[server] = index + 1
				} else {
					rf.leaderStatus.nextIndex[server] = reply.ConflictIndex
				}
			} else {
				rf.leaderStatus.nextIndex[server] = reply.ConflictIndex
			}

			rf.printLog("Append entries to server", server, "failed, decrease nextIndex to", rf.leaderStatus.nextIndex[server])
			rf.issueAppendEntry(server)
		}
	}

	rf.mu.Unlock()
}

// Ensure lock is wrapped
func (rf *Raft) issueAppendEntriesAll() {
	rf.printLog("issue AppendEntries")

	for server := range rf.peers {
		if server != rf.me {
			rf.issueAppendEntry(server)
		}
	}
}

// Ensure lock is wrapped
func (rf *Raft) issueAppendEntry(server int) {
	if rf.status == LEADER {
		nextIndex := rf.leaderStatus.nextIndex[server]
		aea := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			PrevLogIndex: nextIndex - 1,
			PrevLogTerm:  rf.logs[nextIndex-1].Term,
			LeaderID:     rf.me,
			LeaderCommit: rf.commitIndex,
			Entries:      make([]LogEntry, len(rf.logs)-nextIndex),
		}
		copy(aea.Entries, rf.logs[nextIndex:])

		go rf.SendAppendEntries(server, aea)
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.status = FOLLOWER
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.status = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesGot = 1
	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.status = LEADER
	for server := range rf.peers {
		rf.leaderStatus.matchIndex[server] = 0
		rf.leaderStatus.nextIndex[server] = len(rf.logs)
	}
	go rf.Start(nil)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.printLog("Start command")
	if rf.status == LEADER {
		index = len(rf.logs) - rf.numNil
		term = rf.currentTerm
		isLeader = true

		rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
		rf.persist()
		rf.issueAppendEntriesAll()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.printLog("Killed")
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// set and check election timeout
func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(LoopCheckIntervalMS * time.Millisecond)
		rf.mu.Lock()
		if time.Now().Sub(rf.lastTime) > rf.timeoutInterval {
			if rf.status == FOLLOWER || rf.status == CANDIDATE {
				rf.printLog("Timeout, triger next election")
				rf.becomeCandidate()

				rva := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  rf.me,
					LastLogIndex: len(rf.logs) - 1,
					LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
				}
				for server := range rf.peers {
					if server != rf.me {
						go rf.sendRequestVote(server, rva)
					}
				}
				rf.resetTimer()
			}
		}
		rf.mu.Unlock()
	}
}

// periodically send heartbeats
func (rf *Raft) heartBeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status == LEADER {
			rf.issueAppendEntriesAll()
		}
		rf.mu.Unlock()
		time.Sleep(HeartBeatIntervalMS * time.Millisecond)
	}
}

// check and apply all the message from lastApplied to commitIndex
func (rf *Raft) applyLoop() {
	rf.mu.Lock()
	for !rf.killed() {
		rf.applyCond.Wait()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++

			if rf.logs[rf.lastApplied].Command == nil {
				rf.numNil++
				continue
			}
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied - rf.numNil,
			}
			rf.applyCh <- applyMsg
			rf.printLog(rf.lastApplied, "command Applied")
		}
	}
	rf.mu.Unlock()
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
	rand.Seed(time.Now().UnixNano())
	rf := &Raft{}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{0, "init"})
	rf.numNil = 0

	rf.commitIndex = 0
	rf.applyCh = applyCh
	rf.lastApplied = 0
	rf.status = FOLLOWER
	rf.leaderStatus.nextIndex = make([]int, len(rf.peers))
	rf.leaderStatus.matchIndex = make([]int, len(rf.peers))

	rf.mu.Lock()
	// check and apply message between applied and commitIndex in the background
	go rf.applyLoop()

	// leader send periodically empty AppendEntries RPC
	go rf.heartBeatLoop()

	// follower periodically timeout
	rf.resetTimer()
	go rf.electionLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.mu.Unlock()

	return rf
}
