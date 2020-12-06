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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
	TimeoutLowerMS      = 500
	TimeoutUpperMS      = 800
	HeartBeatIntervalMS = 200
	enableLog           = true
)

type leader struct {
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command string
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	votesGot    int
	logs        []LogEntry

	resetTimer bool

	nserver      int
	commitIndex  int
	lastApplied  int
	status       int
	leaderStatus leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

func (rf *Raft) printLog(a ...interface{}) {
	statusMap := map[int]string{
		0: "Foll",
		1: "Cand",
		2: "Lead",
	}

	if enableLog {
		fmt.Print("#", rf.me, " ", statusMap[rf.status], " T", rf.currentTerm, ": ")
		fmt.Println(a...)
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.VoteGranted = false

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = FOLLOWER
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else if rf.status == FOLLOWER {
		if rf.votedFor == -1 {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
			rf.resetTimer = true
			rf.printLog("Grant vote to server", args.CandidateID)
		} else {
			rf.printLog("Reject the vote of server", args.CandidateID, ":I have already voted.")
		}
	}

	rf.mu.Unlock()
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
//
func (rf *Raft) sendRequestVote(server int, rva *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	// TODO: loop call RequestVote if it fails in this term
	ok := rf.peers[server].Call("Raft.RequestVote", rva, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.status = FOLLOWER
			rf.printLog("Reveived higer term in sendRequestVote from server", server, "become Follower")
		}

		if reply.VoteGranted == true {
			if rva.Term == rf.currentTerm && rf.status == CANDIDATE {
				rf.printLog("Get vote from server", server)
				rf.votesGot++
				if rf.votesGot > rf.nserver/2 {
					// become leader
					rf.status = LEADER
					rf.printLog("Become leader")
					rf.sendHeartBeats()
				}
			} else {
				rf.printLog("Get vote from server", server, "but is outdated")
			}
		}
		rf.mu.Unlock()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// rf.printLog("Receive AppendEntries from server", args.LeaderId)
	reply.Success = false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.status = FOLLOWER
		rf.printLog("Reveived higer term in AppendEntries from server", args.LeaderId, "become Follower")
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	} else {
		reply.Success = true
		reply.Term = rf.currentTerm
		rf.resetTimer = true
	}

	rf.mu.Unlock()
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = FOLLOWER
			rf.votedFor = -1
		}
		if reply.Success == true {

		} else {

		}

		rf.mu.Unlock()
	}
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
	isLeader := true

	// Your code here (2B).

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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) electionTimer() {
	trigerNextElection := func() {
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.votesGot = 1
		rva := &RequestVoteArgs{}
		rva.Term = rf.currentTerm
		rva.CandidateID = rf.me
		rva.LastLogIndex = len(rf.logs) - 1
		rva.LastLogTerm = rf.logs[rva.LastLogIndex].Term
		for server := range rf.peers {
			if server != rf.me {
				go rf.sendRequestVote(server, rva)
			}
		}
	}

	for {
		time.Sleep(((time.Duration(rand.Int31()%(TimeoutUpperMS-TimeoutLowerMS) + TimeoutLowerMS)) * time.Millisecond))
		rf.mu.Lock()
		if rf.status == FOLLOWER {
			if rf.resetTimer {
				rf.resetTimer = false
			} else {
				// follower time out, become candidate
				rf.status = CANDIDATE
				rf.printLog("Follower timeout, triger election")
				trigerNextElection()
			}
		} else if rf.status == CANDIDATE {
			// election time out, next round election
			rf.printLog("Candidate timeout, triger next election")
			trigerNextElection()
		} else {
			// leader, do nothing
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) heartBeatTimer() {
	for {
		rf.mu.Lock()
		if rf.status == LEADER {
			rf.sendHeartBeats()
		}
		rf.mu.Unlock()
		time.Sleep(HeartBeatIntervalMS * time.Millisecond)
	}
}

// ensure lock is wrapped
func (rf *Raft) sendHeartBeats() {
	// rf.printLog("Send heart beats to other servers")
	aea := &AppendEntriesArgs{}
	aea.Term = rf.currentTerm
	aea.PrevLogIndex = len(rf.logs) - 1
	aea.PrevLogTerm = rf.logs[aea.PrevLogIndex].Term
	aea.LeaderId = rf.me
	aea.LeaderCommit = rf.commitIndex

	for server := range rf.peers {
		if server != rf.me {
			go rf.SendAppendEntries(server, aea)
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
	rand.Seed(time.Now().UnixNano())
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{0, "init"})

	rf.resetTimer = false
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nserver = len(rf.peers)
	rf.status = FOLLOWER
	rf.leaderStatus.nextIndex = make([]int, rf.nserver)
	for i := range rf.leaderStatus.nextIndex {
		rf.leaderStatus.nextIndex[i] = rf.commitIndex + 1
	}
	rf.leaderStatus.matchIndex = make([]int, rf.nserver)

	// leader send periodically empty AppendEntries RPC
	go rf.heartBeatTimer()

	// follower periodically timeout
	go rf.electionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
