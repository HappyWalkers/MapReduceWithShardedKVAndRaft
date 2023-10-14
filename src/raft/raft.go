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
	"math/rand"
	"slices"
	"sort"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
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

	// persistent state on all servers
	currentTerm uint64 // latest term sever has seen
	votedFor    uint64 // candidateId that received vote in current term
	log         Log    // log entries

	// volatile state on all servers
	commitIndex uint64 // index of highest log entry known to be committed
	lastApplied uint64 // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndexSlice  []uint64 // for each server, index of the next log entry to send to that server
	matchIndexSlice []uint64 // for each server, index of highest log entry known to be replicated on server

	// follower
	electionTimer                                 time.Timer
	mostRecentReceivedAppendEntriesRequestChannel chan bool

	// leader
	role         int
	applyChannel chan ApplyMsg
}

const VOTED_FOR_NO_ONE uint64 = -1
const (
	LEADER    = iota
	CANDIDATE = iota
	FOLLOWER  = iota
)

func getElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(rand.Intn(300)+300)
}

type Log struct {
	logEntrySlice []LogEntry
}

func (log Log) Last() LogEntry {
	return log.logEntrySlice[len(log.logEntrySlice)-1]
}

func (log Log) FindLocationByEntryIndex(entryIndex uint64) (int, bool) {
	return sort.Find(len(log.logEntrySlice), func(i int) int {
		return int(entryIndex - log.logEntrySlice[i].Index)
	})
}

func (log Log) FindEntryByEntryIndex(entryIndex uint64) (LogEntry, bool) {
	loc, found := log.FindLocationByEntryIndex(entryIndex)
	if !found {
		return LogEntry{}, false
	} else {
		return log.logEntrySlice[loc], true
	}
}

type LogEntry struct {
	Term    uint64      // term when entry was received by leader
	Index   uint64      // index of the log entry
	Command interface{} // command for state machine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return int(rf.currentTerm), rf.role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// Invoked by leader to replicate log entries (§5.3); also used as
// heartbeat (§5.2).
type AppendEntriesArgs struct {
	Term            uint64     // leader’s term
	LeaderId        uint64     // so follower can redirect clients
	PrevLogIndex    uint64     // index of log entry immediately preceding new ones
	PrevLogTerm     uint64     // term of prevLogIndex entry
	Entries         []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIdx uint64     // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    uint64 // currentTerm, for leader to update itself
	Success bool   // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (raft Raft) SendAppendEntriesRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := raft.peers[server].Call("Raft.ProcessAppendEntries", args, reply)
	return ok
}

func (raft Raft) ProcessAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// set timeout flag
	raft.electionTimer.Reset(getElectionTimeout())
	if len(raft.mostRecentReceivedAppendEntriesRequestChannel) == 0 {
		raft.mostRecentReceivedAppendEntriesRequestChannel <- true
	}

	// Reply false if term < currentTerm (§5.1)
	if args.Term < raft.currentTerm {
		reply.Success = false
		reply.Term = raft.currentTerm
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	logEntry, ok := raft.log.FindEntryByEntryIndex(args.PrevLogIndex)
	if !ok || logEntry.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = raft.currentTerm
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	slices.SortFunc(args.Entries, func(a LogEntry, b LogEntry) int {
		return int(a.Index - b.Index)
	})
	for _, remoteLogEntry := range args.Entries {
		loc, ok := raft.log.FindLocationByEntryIndex(remoteLogEntry.Index)
		if ok {
			if raft.log.logEntrySlice[loc].Term != remoteLogEntry.Term {
				raft.log.logEntrySlice = raft.log.logEntrySlice[:loc]
			}
		}
	}

	// Append any new entries not already in the log
	for _, remoteLogEntry := range args.Entries {
		_, ok := raft.log.FindLocationByEntryIndex(remoteLogEntry.Index)
		if !ok {
			raft.log.logEntrySlice = append(raft.log.logEntrySlice, remoteLogEntry)
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommitIdx > raft.commitIndex {
		raft.commitIndex = min(args.LeaderCommitIdx, args.Entries[len(args.Entries)-1].Index)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint64 // candidate's term
	CandidateID  uint64 // candidate requesting vote
	LastLogIndex uint64 // index of candidate’s last log entry (§5.4)
	LastLogTerm  uint64 // term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint64 //current term, for candidate to update itself
	voteGranted bool   // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		reply.voteGranted = false
		reply.Term = rf.currentTerm
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == VOTED_FOR_NO_ONE || rf.votedFor == args.CandidateID) &&
		(args.LastLogTerm > rf.log.Last().Term ||
			(args.LastLogTerm == rf.log.Last().Term && args.LastLogIndex >= rf.log.Last().Index)) {
		reply.voteGranted = true
		reply.Term = rf.currentTerm
	}
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
	// Your code here (2B).
	if rf.role != LEADER {
		index := -1
		term := -1
		return index, term, false
	} else {
		// send AppendEntries to all the followers
		appendEntriesSuccessChannel := make(chan bool, len(rf.peers))
		// TODO: append the entries on the leader itself
		for peerIdx, _ := range rf.peers {
			if peerIdx != rf.me {
				go func() {
					logEntry := LogEntry{
						Term:    rf.currentTerm,
						Index:   rf.nextIndexSlice[peerIdx],
						Command: command,
					}
					appendEntriesSuccessChannel = rf.sendAppendEntriesTo(peerIdx, logEntry, appendEntriesSuccessChannel)
				}()
			}
		}
		// if the majority append the entries, apply the command
		// TODO: else failed
		appendEntriesSuccessCount := 0
		for true {
			select {
			case <-appendEntriesSuccessChannel:
				appendEntriesSuccessCount += 1
				if appendEntriesSuccessCount > len(rf.peers)/2 {
					rf.applyChannel <- ApplyMsg{
						CommandValid:  true,
						Command:       command,
						CommandIndex:  int(rf.commitIndex),
						SnapshotValid: false,
						Snapshot:      nil,
						SnapshotTerm:  0,
						SnapshotIndex: 0,
					}
				}
				return int(rf.commitIndex), int(rf.currentTerm), true
			}
		}
	}
}

func (rf *Raft) sendAppendEntriesTo(peerIdx int, logEntry LogEntry, appendEntriesSuccessChannel chan bool) chan bool {
	appendEntriesArgs := AppendEntriesArgs{
		Term:            rf.currentTerm,
		LeaderId:        uint64(rf.me),
		PrevLogIndex:    rf.log.Last().Index,
		PrevLogTerm:     rf.log.Last().Term,
		Entries:         []LogEntry{logEntry},
		LeaderCommitIdx: rf.commitIndex,
	}
	appendEntriesReply := AppendEntriesReply{}
	rf.SendAppendEntriesRequest(peerIdx, &appendEntriesArgs, &appendEntriesReply)
	if appendEntriesReply.Success {
		appendEntriesSuccessChannel <- true
		rf.nextIndexSlice[peerIdx] += 1
		rf.matchIndexSlice[peerIdx] = rf.nextIndexSlice[peerIdx]
	} else {
		rf.nextIndexSlice[peerIdx] -= 1
		logEntry.Index -= 1
		appendEntriesSuccessChannel = rf.sendAppendEntriesTo(peerIdx, logEntry, appendEntriesSuccessChannel)
	}
	// TODO: appendEntriesReply.Term
	return appendEntriesSuccessChannel
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

// TODO: call killed in all goroutines to avoid printing confusing messages
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		// TODO: If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate:
		// convert to candidate
		<-rf.electionTimer.C
		rf.startElection()
	}
}

// Candidates (§5.2):
func (rf *Raft) startElection() {
	// On conversion to candidate, start election:
	rf.role = CANDIDATE
	// • Increment currentTerm
	rf.currentTerm += 1
	// • Vote for self
	voteChannel := make(chan int, len(rf.peers))
	voteChannel <- 1
	// • Reset election timer
	rf.electionTimer.Reset(getElectionTimeout())
	// • Send RequestVote RPCs to all other servers
	for peerIdx, _ := range rf.peers {
		if peerIdx != rf.me {
			go func() {
				requestVoteArgs := RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateID:  uint64(rf.me),
					LastLogIndex: rf.log.Last().Index,
					LastLogTerm:  rf.log.Last().Term,
				}
				requestVoteReply := RequestVoteReply{}
				rf.sendRequestVote(peerIdx, &requestVoteArgs, &requestVoteReply)
				if requestVoteReply.voteGranted {
					voteChannel <- 1
				}
				//TODO:requestVoteReply.Term
			}()
		}
	}

	// If votes received from the majority of servers: become leader
	// If AppendEntries RPC received from new leader: convert to follower
	// If election timeout elapses: start new election
	voteSum := 0
	for len(rf.mostRecentReceivedAppendEntriesRequestChannel) > 0 { // clean the channel
		<-rf.mostRecentReceivedAppendEntriesRequestChannel
	}
	for true {
		select {
		case vote := <-voteChannel:
			voteSum += vote
			if voteSum > len(rf.peers)/2 {
				rf.role = LEADER
				rf.Lead()
				return
			}
		case <-rf.mostRecentReceivedAppendEntriesRequestChannel:
			rf.role = FOLLOWER
			return
		case <-rf.electionTimer.C:
			rf.startElection()
		}
	}
}

const HEARTBEAT_TIMEOUT = time.Millisecond * 100

func (raft *Raft) Lead() {
	//Upon election: send initial empty AppendEntries RPCs
	//(heartbeat) to each server; repeat during idle periods to
	//prevent election timeouts (§5.2)
	go func() {
		heartbeatTicker := time.NewTicker(HEARTBEAT_TIMEOUT)
		for raft.killed() == false {
			select {
			case <-heartbeatTicker.C:
				raft.sendHeartbeat()
				// TODO: stop sending heartbeat when the leader become a follower
			}
		}
	}()
}

func (raft *Raft) sendHeartbeat() {
	for peerIdx, _ := range raft.peers {
		if peerIdx != raft.me {
			appendEntriesArgs := AppendEntriesArgs{
				Term:            raft.currentTerm,
				LeaderId:        uint64(raft.me),
				PrevLogIndex:    raft.log.Last().Index,
				PrevLogTerm:     raft.log.Last().Term,
				Entries:         []LogEntry{},
				LeaderCommitIdx: raft.commitIndex,
			}
			appendEntriesReply := AppendEntriesReply{}
			raft.SendAppendEntriesRequest(peerIdx, &appendEntriesArgs, &appendEntriesReply)
		}
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChannel = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
