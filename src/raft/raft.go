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
	"slices"
	"sort"
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
	CurrentTerm uint64 // latest term sever has seen
	VotedFor    uint64 // candidateId that received vote in current term
	Log         Log    // log entries

	// volatile state on all servers
	CommitIndex uint64 // index of highest log entry known to be committed
	LastApplied uint64 // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndexSlice  []uint64 // for each server, index of the next log entry to send to that server
	matchIndexSlice []uint64 // for each server, index of highest log entry known to be replicated on server
}

const VOTED_FOR_NO_ONE uint64 = -1

type Log struct {
	LogEntrySlice []LogEntry
}

func (log Log) First() LogEntry {
	return log.LogEntrySlice[0]
}

func (log Log) Last() LogEntry {
	return log.LogEntrySlice[len(log.LogEntrySlice)-1]
}

func (log Log) FindLocationByEntryIndex(entryIndex uint64) (int, bool) {
	return sort.Find(len(log.LogEntrySlice), func(i int) int {
		return int(entryIndex - log.LogEntrySlice[i].Index)
	})
}

func (log Log) FindEntryByEntryIndex(entryIndex uint64) (LogEntry, bool) {
	loc, found := log.FindLocationByEntryIndex(entryIndex)
	if !found {
		return LogEntry{}, false
	} else {
		return log.LogEntrySlice[loc], true
	}
}

type LogEntry struct {
	Term    uint64 // term when entry was received by leader
	Index   uint64 // index of the log entry
	Command []byte // command for state machine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	return term, isleader
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

func (raft Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := raft.peers[server].Call("Raft.ProcessAppendEntries", args, reply)
	return ok
}

func (raft Raft) ProcessAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Reply false if term < currentTerm (§5.1)
	if args.Term < raft.CurrentTerm {
		reply.Success = false
		reply.Term = raft.CurrentTerm
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	logEntry, ok := raft.Log.FindEntryByEntryIndex(args.PrevLogIndex)
	if !ok || logEntry.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = raft.CurrentTerm
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	slices.SortFunc(args.Entries, func(a LogEntry, b LogEntry) int {
		return int(a.Index - b.Index)
	})
	for _, remoteLogEntry := range args.Entries {
		loc, ok := raft.Log.FindLocationByEntryIndex(remoteLogEntry.Index)
		if ok {
			if raft.Log.LogEntrySlice[loc].Term != remoteLogEntry.Term {
				raft.Log.LogEntrySlice = raft.Log.LogEntrySlice[:loc]
			}
		}
	}

	// Append any new entries not already in the log
	for _, remoteLogEntry := range args.Entries {
		_, ok := raft.Log.FindLocationByEntryIndex(remoteLogEntry.Index)
		if !ok {
			raft.Log.LogEntrySlice = append(raft.Log.LogEntrySlice, remoteLogEntry)
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommitIdx > raft.CommitIndex {
		raft.CommitIndex = min(args.LeaderCommitIdx, args.Entries[len(args.Entries)-1].Index)
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint64 // candidate's term
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
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
	if args.Term < rf.CurrentTerm {
		reply.voteGranted = false
		reply.Term = rf.CurrentTerm
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.VotedFor == VOTED_FOR_NO_ONE || rf.VotedFor == args.CandidateID) &&
		(args.LastLogTerm > rf.Log.Last().Term ||
			(args.LastLogTerm == rf.Log.Last().Term && args.LastLogIndex >= rf.Log.Last().Index)) {
		reply.voteGranted = true
		reply.Term = rf.CurrentTerm
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
