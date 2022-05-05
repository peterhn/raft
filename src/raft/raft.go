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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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
	Value interface{}
	Term  int
}

type State int

const (
	Follower  State = 0
	Candidate       = 1
	Leader          = 2
)

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
	logEntries  []*LogEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	leaderMessageTimestamp int64
	electionTimeout        int64
	state                  State

	applyMessge chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()

	return term, isleader
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term          int
	VoteGranted   bool
	ReplyReceived bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// colorGreen := "\033[32m"

	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	DPrintf("peerID: %v, voting for term: %v candidate: %v", rf.me, args.Term, args.CandidateId)
	DPrintf("peerID: %v, current term: %v, current index: %v", rf.me, rf.currentTerm, rf.commitIndex)
	DPrintf("peerID: %v, last term: %v, last index: %v", rf.me, args.LastLogTerm, args.LastLogIndex)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	DPrintf("peerID: %v, votedFor: %v", rf.me, rf.votedFor)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogIndex >= rf.commitIndex && args.LastLogTerm >= rf.currentTerm) {
		rf.votedFor = args.CandidateId
		rf.state = Follower
		now := time.Now()
		rf.leaderMessageTimestamp = now.UnixMilli()
		reply.Term = args.Term
		reply.VoteGranted = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	rf.mu.Lock()
	commitIndex := rf.commitIndex
	term := rf.currentTerm
	lastApplied := rf.lastApplied
	isLeader := rf.state == Leader
	rf.mu.Unlock()

	if isLeader {
		logEntry := LogEntry{
			Value: command,
			Term:  term,
		}

		// Your code here (2B).
		// fmt.Printf("Received Message: %v (Leader: %v, Term: %v, CommitIndex: %v)\n", command, isLeader, term, index)
		rf.replicateLogsAllPeers(logEntry, commitIndex, lastApplied, term)
	}

	return commitIndex, term, isLeader
}

func (rf *Raft) replicateLogsAllPeers(logEntry LogEntry, commitIndex, lastApplied, term int) {
	rf.mu.Lock()
	nextIndexes := rf.nextIndex
	matchIndexes := rf.matchIndex
	rf.logEntries = append(rf.logEntries, &logEntry)
	rf.lastApplied += 1
	logEntries := rf.logEntries
	majority := (len(rf.peers) + 1) / 2
	successResponse := 0
	commitApplied := false
	rf.mu.Unlock()

	var replies []AppendEntriesReply
	c := make(chan AppendEntriesReply)
	go func(replyCh chan AppendEntriesReply) {
		successResponse += 1
		reply := <-replyCh
		if successResponse >= majority && !commitApplied {
			rf.mu.Lock()
			rf.commitIndex += 1
			commitApplied = true
			rf.matchIndex[reply.PeerIndex] += 1
			rf.nextIndex[reply.PeerIndex] += 1
			rf.mu.Unlock()
		}
		if reply.Term > term {

		}
		replies = append(replies)
	}(c)

	for i := 0; i < len(rf.peers); i++ {
		prevLogIndex := matchIndexes[i]
		prevLogTerm := logEntries[prevLogIndex].Term
		go sendLogEntry(rf, i, prevLogIndex, prevLogTerm, term, commitIndex, matchIndexes[i], nextIndexes[i], logEntries, c)
	}

	DPrintf("replies: %v", replies)
}

func sendLogEntry(rf *Raft, peerIndex, prevLogIndex, prevLogTerm, term, commitIndex, matchIndex, nextIndex int, logEntries []*LogEntry, c chan AppendEntriesReply) {
	reply := AppendEntriesReply{}
	reply.PeerIndex = peerIndex
	var request = &AppendEntriesRequest{
		Term:         term,
		LeaderId:     rf.me,
		LeaderCommit: commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries[matchIndex:nextIndex],
	}

	rf.sendAppendEntries(peerIndex, request, &reply)
	if reply.Success {
		c <- reply
	} else {
		// retry
		rf.mu.Lock()
		rf.nextIndex[peerIndex] -= 1
		nextIndex = rf.nextIndex[peerIndex]
		rf.mu.Unlock()
		sendLogEntry(rf, peerIndex, prevLogIndex, prevLogTerm, term, commitIndex, matchIndex, nextIndex, logEntries, c)
	}
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
	DPrintf("peer %v killed\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	DPrintf("Ticking Leader Election for %v\n", rf.me)
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Duration(rf.electionTimeout) * time.Millisecond)
		rf.mu.Lock()
		now := time.Now()
		ts := now.UnixMilli()
		diff := ts - rf.leaderMessageTimestamp
		DPrintf("checking timeout for: %v, diff: %v, last: %v, curr: %v", rf.me, diff, rf.leaderMessageTimestamp, ts)

		if diff > rf.electionTimeout {
			DPrintf("Election timeout for %v\n", rf.me)
			rf.votedFor = rf.me
			rf.state = Candidate
		}
		isCandidate := rf.state == Candidate
		rf.mu.Unlock()

		if isCandidate {
			rf.mu.Lock()
			rf.currentTerm += 1
			request := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.commitIndex,
				LastLogTerm:  rf.currentTerm,
			}
			rf.mu.Unlock()

			electionWon := rf.sendRequestVotesAllPeers(request)

			if electionWon {
				rf.mu.Lock()
				rf.state = Leader
				rf.nextIndex = make([]int, rf.lastApplied+1)
				rf.mu.Unlock()
				DPrintf("Candidate %v has won the election in term %v\n", rf.votedFor, rf.currentTerm)
				rf.tickHeartbeats()
			}
		}

	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeatAllPeers() {
	rf.mu.Lock()
	termHeartbeat := rf.currentTerm
	termCommitIndex := rf.commitIndex
	// electionTimeout := rf.electionTimeout
	now := time.Now()
	rf.leaderMessageTimestamp = now.UnixMilli()
	numPeers := len(rf.peers)
	rf.mu.Unlock()
	var request = &AppendEntriesRequest{
		Term:         termHeartbeat,
		LeaderId:     rf.me,
		LeaderCommit: termCommitIndex,
	}

	maxTerm := termHeartbeat
	wg := sync.WaitGroup{}
	wg.Add(numPeers - 1)

	lock := sync.Mutex{}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		peerId := i
		go func() {
			var reply = &AppendEntriesReply{}
			rf.sendAppendEntries(peerId, request, reply)
			lock.Lock()
			maxTerm = Max(maxTerm, reply.Term)
			lock.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()
	// waitTimeout(&wg, time.Duration(electionTimeout)*time.Millisecond)

	if maxTerm > termHeartbeat {
		rf.mu.Lock()
		// move to follower
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = maxTerm
		rf.mu.Unlock()
	}
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}

func getRandomRange(lower, upper int) (number int64) {
	number = int64(lower + rand.Intn(upper-lower+1))
	return number
}

func (rf *Raft) tickHeartbeats() {
	var tickTime = 50
	DPrintf("starting heartbeats for: %v votedFor: %v", rf.me, rf.votedFor)
	_, isLeader := rf.GetState()
	for isLeader {
		go rf.sendHeartbeatAllPeers()
		time.Sleep(time.Duration(tickTime) * time.Millisecond)
		_, isLeader = rf.GetState()
	}
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (rf *Raft) sendRequestVotesAllPeers(request *RequestVoteArgs) bool {
	votesGranted, received := 1, 1
	wg := sync.WaitGroup{}
	wg.Add(1)
	lock := sync.Mutex{}
	done := false
	electionWon := false

	rf.mu.Lock()
	termHeartbeat := rf.currentTerm
	maxTerm := rf.currentTerm
	majority := (len(rf.peers) + 1) / 2
	numPeers := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		peerId := i
		go func() {
			DPrintf("sending vote request for %v to peer %v for term %v...", request.CandidateId, peerId, request.Term)
			reply := RequestVoteReply{}
			rf.sendRequestVote(peerId, request, &reply)
			DPrintf(" received %v for candidate %v from peer %v for term %v\n", reply.VoteGranted, rf.me, peerId, request.Term)
			//DPrintf("received reply: %v", reply)
			lock.Lock()
			received += 1
			maxTerm = Max(maxTerm, reply.Term)
			if reply.VoteGranted {
				votesGranted += 1
			}
			if (votesGranted >= majority || received == numPeers) && !done {
				done = true
				electionWon = votesGranted >= majority
				wg.Done()
			}
			lock.Unlock()
		}()
	}

	wg.Wait()

	lock.Lock()
	largerTerm := maxTerm > termHeartbeat
	DPrintf("candidate %v: granted %v out of majority %v", rf.me, votesGranted, majority)
	lock.Unlock()
	if largerTerm {
		rf.mu.Lock()
		// move to follower
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = maxTerm
		rf.mu.Unlock()
		return false
	}

	return electionWon
}

type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []*LogEntry
}

type AppendEntriesReply struct {
	// Your data here (2A).
	PeerIndex int
	Term      int
	Success   bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	logEntries := rf.logEntries
	logLength := len(rf.logEntries)
	rf.mu.Unlock()

	if args.Term < currentTerm || args.PrevLogIndex >= logLength {
		reply.Success = false
		reply.Term = currentTerm
		return
	}

	logEntry := logEntries[args.PrevLogIndex]
	if logEntry.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = currentTerm
		return
	}

	rf.mu.Lock()
	if args.Term > currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.state = Follower
	}
	rf.mu.Unlock()

	j := args.PrevLogIndex + 1
	for i := 0; i < len(args.Entries); i++ {
		logEntry := args.Entries[i]

		if j < len(logEntries) {
			logEntries[j] = logEntry
		} else {
			logEntries = append(logEntries, logEntry)
		}
		j += 1
	}

	rf.mu.Lock()
	rf.logEntries = logEntries
	rf.commitIndex = Min(args.LeaderCommit, rf.lastApplied)
	// DPrintf("heartbeat received from leader %v f r follower %v and term %v voted for %v\n", args.LeaderId, rf.me, rf.currentTerm, rf.votedFor)
	now := time.Now()
	rf.leaderMessageTimestamp = now.UnixMilli()
	rf.mu.Unlock()
	reply.Term = currentTerm
	reply.Success = true
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

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.electionTimeout = getRandomRange(150, 300)
	rf.applyMessge = applyCh

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 0)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
