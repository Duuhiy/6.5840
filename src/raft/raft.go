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
	"6.5840/labgob"
	"bytes"
	"math/rand"
	"time"

	"6.5840/labrpc"
	"sync"
	"sync/atomic"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Ind     int
	Command interface{}
}

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	voteFor     int
	logs        []LogEntry
	// Volatile state on all servers
	commitedIndex int
	lastApplied   int
	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int
	matchIndex []int

	role        Role
	applyCh     chan ApplyMsg
	votedCount  int
	winElectCh  chan bool
	stepDownCh  chan bool
	grantVoteCh chan bool
	heartbeatCh chan bool

	lastSnapshotIndex int
	lastSnapshotTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.voteFor) != nil || e.Encode(rf.currentTerm) != nil || e.Encode(rf.logs) != nil || e.Encode(rf.lastSnapshotIndex) != nil || e.Encode(rf.lastSnapshotTerm) != nil {
		//if e.Encode(rf.currentTerm) != nil || e.Encode(rf.voteFor) != nil || e.Encode(rf.logs) != nil {
		panic("failed to encode raft persistent state")
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.persister.snapshot)
}

// restore previously persisted state.
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
	//fmt.Printf("server %d readPersist\n", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	var lastSnapshotIndex int
	var lastSnapshotTerm int
	if d.Decode(&voteFor) != nil || d.Decode(&currentTerm) != nil || d.Decode(&logs) != nil || d.Decode(&lastSnapshotIndex) != nil || d.Decode(&lastSnapshotTerm) != nil {
		//if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		panic("failed to decode raft persistent state")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
		rf.lastSnapshotIndex = lastSnapshotIndex
		rf.lastSnapshotTerm = lastSnapshotTerm
		//fmt.Printf("server %d currentTerm = %d len(logs) = %d lastSnapshotIndex = %d lastSnapshotTerm = %d \n", rf.me, rf.currentTerm, len(rf.logs), rf.lastSnapshotIndex, rf.lastSnapshotTerm)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer fmt.Printf("%d 号 server 的 lastSnapshotIndex = %d len(logs) = %d \n ", rf.me, rf.lastSnapshotIndex, len(rf.logs))
	//fmt.Printf("Snapshot index = %d, %d 号 server 的 lastSnapshotIndex = %d len(logs) = %d \n ", index, rf.me, rf.lastSnapshotIndex, len(rf.logs))
	if rf.lastSnapshotIndex >= index {
		//fmt.Println("Snapshot 被调用了 rf.lastSnapshotIndex >= index")
		return
	}

	if index > rf.commitedIndex {
		//fmt.Println("snapshot index shouldn't surpass the commitedIndex")
		//return
		panic("snapshot index shouldn't surpass the commitedIndex")
	}
	// 不超过commitedIndex，就不会超过len(logs) + lastSnapshotIndex
	// 只保留index后面的log
	rf.lastSnapshotTerm = rf.logs[rf.getIndex(index)].Term
	rf.logs = rf.logs[rf.getIndex(index):]
	rf.lastSnapshotIndex = index
	//fmt.Printf("Snapshot 被调用了 截断了日志, %d 号 server 的 lastSnapshotIndex = %d len(logs) = %d \n ", rf.me, rf.lastSnapshotIndex, len(rf.logs))
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.voteFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.logs)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	//e.Encode(rf.lastSnapshotTerm)
	//totalSnapshot := append(w.Bytes(), snapshot...)
	rf.persister.Save(w.Bytes(), snapshot)
}

func (rf *Raft) applyLogs() {
	for !rf.killed() {
		rf.mu.Lock()

		oldApplied, oldCommited := rf.lastApplied, rf.commitedIndex
		if oldApplied < rf.lastSnapshotIndex {
			// lastSnapshotIndex <= commitedIndex = lastApplied 说明经过了crash
			rf.lastApplied = rf.lastSnapshotIndex
			rf.commitedIndex = rf.lastSnapshotIndex
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}

		if oldCommited < rf.lastSnapshotIndex {
			rf.commitedIndex = rf.lastSnapshotIndex
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}

		if oldCommited == oldApplied || (oldCommited-oldApplied) >= len(rf.logs) {
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 30)
			continue
		}

		entries := make([]LogEntry, oldCommited-oldApplied)
		copy(entries, rf.logs[rf.getIndex(oldApplied)+1:rf.getIndex(oldCommited)+1])
		rf.mu.Unlock()
		for i, log := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				CommandIndex: i + oldApplied + 1,
				Command:      log.Command,
			}
		}

		rf.mu.Lock()
		if rf.lastApplied < oldCommited {
			rf.lastApplied = oldCommited
		}
		if rf.lastApplied > rf.commitedIndex {
			rf.commitedIndex = rf.lastApplied
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 30)
	}
	//defer fmt.Println("applyLogs 完成")
	//fmt.Println("进入 applyLogs")
}

func (rf *Raft) applyLogsBySnapshot() {
	//fmt.Println("applyLogsBySnapshot 被调用了")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotIndex: rf.lastSnapshotIndex,
		SnapshotTerm:  rf.lastSnapshotTerm,
	}
	rf.lastApplied = rf.lastSnapshotIndex
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		//fmt.Printf("RequestVote %d server 从leader变回了follower\n", rf.me)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	//isUpToDate := true
	isUpToDate := false
	if args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= rf.lastSnapshotIndex+len(rf.logs)-1 {
		isUpToDate = true
	} else {
		isUpToDate = args.LastLogTerm > rf.logs[len(rf.logs)-1].Term
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && isUpToDate {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.sendToChannel(rf.grantVoteCh, true)
		//fmt.Printf("%d 号 server 收到了来自 %d 号 server 的投票\n", args.CandidateId, rf.me)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []LogEntry
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
	NextTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = -1
		//fmt.Println("AppendEntries, args.Term < rf.currentTerm")
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	rf.sendToChannel(rf.heartbeatCh, true)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.NextTerm = -1

	lastIndex := rf.lastSnapshotIndex + len(rf.logs) - 1
	//fmt.Println("last : ",lastIndex)
	if args.PrevLogIndex > lastIndex {
		reply.NextIndex = lastIndex + 1
		//fmt.Printf("%d 号 server 给 %d 号 server 发送 entries，reply.NextIndex = lastIndex + 1\n", args.LeaderId, rf.me)
		//fmt.Println(reply.NextIndex)
		return
	}

	if pTerm := rf.logs[rf.getIndex(args.PrevLogIndex)].Term; pTerm != args.PrevLogTerm {
		// term不相等，回退
		reply.NextTerm = pTerm
		// 找到conflict term的第一条
		for i := rf.getIndex(args.PrevLogIndex); i >= 0 && rf.logs[i].Term == pTerm; i-- {
			reply.NextIndex = i
		}
		//fmt.Println(reply.NextIndex)
		//fmt.Printf("%d 号 server 给 %d 号 server 发送 entries，pTerm != args.PrevLogTerm, reply.NextIndex = %d \n", args.LeaderId, rf.me, reply.NextIndex)
		return
	}

	if args.Entries != nil {
		//fmt.Println(len(args.Entries))
		i, j := rf.getIndex(args.PrevLogIndex+1), 0
		for ; i < len(rf.logs) && j < len(args.Entries); i, j = i+1, j+1 {
			if rf.logs[i].Term != args.Entries[j].Term {
				break
			}
		}
		rf.logs = rf.logs[:i] // i = 1, j = 0
		rf.logs = append(rf.logs, args.Entries[j:]...)
		//fmt.Println(len(rf.logs))

	}
	//reply.NextIndex = len(rf.logs)
	//fmt.Printf("AppendEntries %d 号 server 同步了log， lastSnapshotIndex = %d len(rf.logs) = %d\n", rf.me, rf.lastSnapshotIndex, len(rf.logs))
	reply.Success = true

	if args.LeaderCommit > rf.commitedIndex {
		//fmt.Printf("%d 号 server 给 %d 号 server 发送 entries，apply \n", args.LeaderId, rf.me)
		if args.LeaderCommit < lastIndex {
			rf.commitedIndex = args.LeaderCommit
		} else {
			rf.commitedIndex = lastIndex
		}
		//go rf.applyLogs()
	}
}

type InstallSanpshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSanpshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSanpshotArgs, reply *InstallSanpshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		//fmt.Println("args.Term < rf.currentTerm")
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		//fmt.Println("args.Term > rf.currentTerm")
	}

	reply.Term = rf.currentTerm

	if rf.lastSnapshotIndex >= args.LastIncludedIndex {
		//fmt.Println("rf.lastSnapshotIndex >= args.LastIncludedIndex")
		return
	}
	rf.sendToChannel(rf.heartbeatCh, true)

	lastIndex := rf.lastSnapshotIndex + len(rf.logs) - 1
	start := args.LastIncludedIndex - rf.lastSnapshotIndex
	if args.LastIncludedIndex <= lastIndex && rf.logs[start].Term == args.LastIncludedTerm {
		// 保留后面的日志
		//fmt.Println("保留部分")
		rf.logs = rf.logs[start:]
	} else {
		rf.logs = make([]LogEntry, 1)
		rf.logs[0] = LogEntry{
			Term: args.LastIncludedTerm,
			Ind:  args.LastIncludedIndex,
		}
	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.lastSnapshotTerm = args.LastIncludedTerm
	rf.persister.Save(rf.persister.ReadRaftState(), args.Data)
	//fmt.Printf("InstallSnapshot %d 号 server 同步了log， lastSnapshotIndex = %d len(rf.logs) = %d\n", rf.me, rf.lastSnapshotIndex, len(rf.logs))
	rf.commitedIndex = rf.lastSnapshotIndex
	go rf.applyLogsBySnapshot()
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

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		//fmt.Printf("sendRequestVote %d server 从leader变回了follower\n", rf.me)
		rf.persist()
		return ok
	}

	if reply.VoteGranted {
		rf.votedCount++
		if rf.votedCount == len(rf.peers)/2+1 {
			// 通知选举当选
			rf.sendToChannel(rf.winElectCh, true)
			//rf.winElectCh <- true
		}
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.role != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		return ok
	}

	if reply.Success {
		newMatchIndex := args.PrevLogIndex + len(args.Entries)
		if newMatchIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatchIndex
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else if reply.NextTerm < 0 {
		rf.nextIndex[server] = reply.NextIndex
		//rf.matchIndex[server] = reply.NextIndex - 1
	} else {
		// 找到最后一个>=reply.nextTerm
		prevLogIndex := rf.getIndex(args.PrevLogIndex)
		for ; prevLogIndex >= 0; prevLogIndex-- {
			if rf.logs[prevLogIndex].Term == reply.NextTerm {
				break
			}
		}
		if prevLogIndex < 0 {
			rf.nextIndex[server] = reply.NextIndex
		} else {
			rf.nextIndex[server] = prevLogIndex + rf.lastSnapshotIndex
			//fmt.Println(rf.nextIndex[server])
		}

		//rf.matchIndex[server] = rf.nextIndex[server] - 1
	}
	//fmt.Println("newMatchIndex = ", rf.matchIndex[server])
	for i := len(rf.logs) - 1; i > rf.commitedIndex-rf.lastSnapshotIndex; i-- {
		cnt := 1
		if rf.logs[i].Term == rf.currentTerm {
			for j, _ := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i {
					cnt++
				}
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitedIndex = i + rf.lastSnapshotIndex
			//go rf.applyLogs()
			break
		}
	}

	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSanpshotArgs, reply *InstallSanpshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return ok
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader || args.Term != rf.currentTerm || reply.Term < rf.currentTerm {
		return ok
	}

	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
		return ok
	}

	if args.LastIncludedIndex > rf.matchIndex[server] {
		rf.matchIndex[server] = args.LastIncludedIndex
	}

	if args.LastIncludedIndex+1 > rf.nextIndex[server] {
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}

	for i := len(rf.logs) - 1; i > rf.commitedIndex; i-- {
		cnt := 1
		if rf.logs[i].Term == rf.currentTerm {
			for j, _ := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i {
					cnt++
				}
			}
		}
		if cnt > len(rf.peers)/2 {
			rf.commitedIndex = i + rf.lastSnapshotIndex
			//go rf.applyLogs()
			break
		}
	}

	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.role == Leader {
		isLeader = true
		index = rf.lastSnapshotIndex + len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{
			Command: command,
			Term:    term,
			Ind:     index,
		})
		rf.persist()
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	//fmt.Println("开始ticker")
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 100
		// milliseconds.
		rf.mu.Lock()
		state := rf.role
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.stepDownCh:
			case <-time.After(120 * time.Millisecond):
				go rf.broadcastAppendEntries()
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
			case <-rf.winElectCh:
				rf.convertToLeader()
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(Candidate) //
			}
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(Follower)
			}
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return
	}
	//fmt.Printf("broadcastAppendEntries, %d 号 server 的 lastSnapshotIndex = %d len(logs) = %d commitedIndex = %d \n ", rf.me, rf.lastSnapshotIndex, len(rf.logs), rf.commitedIndex)
	//fmt.Printf("%d 号server heartbeat\n", rf.me)
	for i, _ := range rf.peers {
		if i != rf.me {
			if rf.nextIndex[i] <= rf.lastSnapshotIndex {
				//fmt.Printf("%d 号 server 给 %d 号server 发送 sendInstallSnapshot\n", rf.me, i)
				args := InstallSanpshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastSnapshotIndex,
					LastIncludedTerm:  rf.lastSnapshotTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				reply := InstallSanpshotReply{}

				go rf.sendInstallSnapshot(i, &args, &reply)
			} else {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					LeaderCommit: rf.commitedIndex,
				}
				args.PrevLogIndex = rf.nextIndex[i] - 1
				//fmt.Println(args.PrevLogIndex)
				args.PrevLogTerm = rf.logs[rf.getIndex(args.PrevLogIndex)].Term
				entries := rf.logs[rf.getIndex(rf.nextIndex[i]):]
				args.Entries = make([]LogEntry, len(entries))
				copy(args.Entries, entries)
				reply := AppendEntriesReply{}
				//fmt.Printf("%d 号 server 给 %d 号server 发送 sendAppendEntries\n", rf.me, i)
				go rf.sendAppendEntries(i, &args, &reply)
			}
		}
	}
}

func (rf *Raft) getIndex(index int) int {
	return index - rf.lastSnapshotIndex
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(360 + rand.Intn(340))
}

func (rf *Raft) convertToCandidate(fromStste Role) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != fromStste {
		return
	}
	//fmt.Printf("%d 号 server 成为candidate\n", rf.me)
	rf.resetChannels()
	rf.role = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	rf.votedCount = 1
	rf.persist()

	go rf.broadcastRequestVote()
}

func (rf *Raft) sendToChannel(ch chan bool, value bool) {
	// 直接 ch <- true 可能会阻塞
	select {
	case ch <- value:
	default:
	}
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Candidate {
		return
	}
	//fmt.Printf("%d 号 server成为了leader\n", rf.me)
	rf.resetChannels()
	rf.role = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.lastSnapshotIndex + len(rf.logs)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastIndex
	}
	//rf.persist()

	//go rf.broadcastAppendEntries()
}

func (rf *Raft) resetChannels() {
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
}

func (rf *Raft) broadcastRequestVote() {
	// 从convertToCandidate来，已经加锁了，不能再加锁
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Candidate {
		return
	}
	//fmt.Printf("%d 号 server 广播拉票\n", rf.me)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.lastSnapshotIndex + len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}

	for i, _ := range rf.peers {
		if i != rf.me {
			reply := RequestVoteReply{}
			//fmt.Printf("%d 号 server 给 %d 号 server发送投票请求\n", rf.me, i)
			go rf.sendRequestVote(i, &args, &reply)
		}
	}
}

func (rf *Raft) stepDownToFollower(term int) {
	// 调用该方法之前已经加锁了
	state := rf.role
	rf.role = Follower
	rf.voteFor = -1
	rf.currentTerm = term
	rf.persist()
	if state != Follower {
		// 通知变成follower
		rf.sendToChannel(rf.stepDownCh, true)
		//rf.stepDownCh <- true
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.role = Follower
	rf.logs = append(rf.logs, LogEntry{Term: 0})
	//fmt.Println(len(rf.logs))
	rf.commitedIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = nil
	rf.matchIndex = nil
	rf.applyCh = applyCh
	rf.voteFor = -1
	rf.votedCount = 0
	rf.winElectCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//rf.recoverFromSnapshot(rf.persister.snapshot)
	//rf.applyLogsBySnapshot()
	//fmt.Println("applyLogsBySnapshot 第一次调用完成")
	// start ticker goroutine to start elections
	//fmt.Println("make完成")
	go rf.ticker()
	go rf.applyLogs()
	return rf
}
