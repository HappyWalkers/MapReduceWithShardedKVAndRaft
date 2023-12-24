package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, value, isleader)
//   start agreement on a new log entry
// rf.GetState() (value, isLeader)
//   ask a Raft for its current value, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/dLog"
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"sync"
	"sync/atomic"
)

// TODO: However, Figure 2 generally doesn’t discuss what you should do when you
// get old RPC replies. From experience, we have found that by far the simplest
// thing to do is to first record the term in the reply (it may be higher than
// your current term), and then to compare the current term with the term you
// sent in your original RPC. If the two are different, drop the reply and
// return. Only if the two terms are the same should you continue processing the
// reply. There may be further optimizations you can do here with some clever
// protocol reasoning, but this approach seems to work well. And not doing it
// leads down a long, winding path of blood, sweat, tears and despair.

// TODO: Keep in mind that the network can delay RPCs and RPC replies, and when
// you send concurrent RPCs, the network can re-order requests and
// replies. Figure 2 is pretty good about pointing out places where RPC
// handlers have to be careful about this (e.g. an RPC handler should
// ignore RPCs with old terms). Figure 2 is not always explicit about RPC
// reply processing. The leader has to be careful when processing
// replies; it must check that the term hasn't changed since sending the
// RPC, and must account for the possibility that replies from concurrent
// RPCs to the same follower have changed the leader's state (e.g.
// nextIndex).

// A Go object implementing a single Raft peer.
// When acquiring locks for many objects,
// the order of acquiring should be the same to the order of these variables written down in the Raft struct
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
	currentTerm1 ValueWithRWMutex[int] // latest value sever has seen
	votedFor2    ValueWithRWMutex[int] // candidateId that received vote in current value
	log3         ValueWithRWMutex[Log] // log entries

	// volatile state on all servers
	role4        ValueWithRWMutex[int] // role of the server
	roleChannel5 chan int              // channel for sending role
	commitIndex6 ValueWithRWMutex[int] // index of highest log entry known to be committed
	lastApplied7 ValueWithRWMutex[int] // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndexSlice8  []ValueWithRWMutex[int] // for each server, index of the next log entry to send to that server
	matchIndexSlice9 []ValueWithRWMutex[int] // for each server, index of highest log entry known to be replicated on server

	// follower
	//TODO: The management of the election timeout is a common source of
	//headaches. Perhaps the simplest plan is to maintain a variable in the
	//Raft struct containing the last time at which the peer heard from the
	//leader, and to have the election timeout goroutine periodically check
	//to see whether the time since then is greater than the timeout period.
	//It's easiest to use time.Sleep() with a small constant argument to
	//drive the periodic checks. Don't use time.Ticker and time.Timer;
	//they are tricky to use correctly.
	//https://stackoverflow.com/questions/37962666/how-does-golang-ticker-work
	electionTimer10 *time.Ticker

	// leader
	//todo: You'll want to have a separate long-running goroutine that sends
	//committed log entries in order on the applyCh. It must be separate,
	//since sending on the applyCh can block; and it must be a single
	//goroutine, since otherwise it may be hard to ensure that you send log
	//entries in log order. The code that advances commitIndex will need to
	//kick the apply goroutine; it's probably easiest to use a condition
	//variable (Go's sync.Cond) for this.
	applyChannel11 chan ApplyMsg

	// snapshot
	snapshot12 ValueWithRWMutex[Snapshot]
}

// votedFor
const VOTED_FOR_NO_ONE int = -1

// role
const (
	LEADER    = iota
	CANDIDATE = iota
	FOLLOWER  = iota
)

// dead
const (
	LIVE = iota
	DEAD = iota
)

// TODO: it may be a good idea to create a lock manager like the context manager in python or std::lock in cpp
// the context manager can release resources opened
// the lock manager may have the same function
// A function that takes a function to be executed is responsible for lock, read/write, and unlock
// If multiple locks are needed, a general interface like the stream in cpp may be useful
type ValueWithRWMutex[T any] struct {
	varName string
	Value   T
	rwMutex sync.RWMutex
}

func (valueWithRWMutex *ValueWithRWMutex[T]) RLock() {
	//dLog.Debug(dLog.DLock, "%v is waiting for the RLock", valueWithRWMutex.varName)
	valueWithRWMutex.rwMutex.RLock()
	//dLog.Debug(dLog.DLock, "%v obtained the RLock", valueWithRWMutex.varName)
}

func (valueWithRWMutex *ValueWithRWMutex[T]) RUnlock() {
	valueWithRWMutex.rwMutex.RUnlock()
	//dLog.Debug(dLog.DLock, "%v RUnlock", valueWithRWMutex.varName)
}

func (valueWithRWMutex *ValueWithRWMutex[T]) Lock() {
	//dLog.Debug(dLog.DLock, "%v is waiting for the Lock", valueWithRWMutex.varName)
	valueWithRWMutex.rwMutex.Lock()
	//dLog.Debug(dLog.DLock, "%v obtained the Lock", valueWithRWMutex.varName)
}

func (valueWithRWMutex *ValueWithRWMutex[T]) Unlock() {
	valueWithRWMutex.rwMutex.Unlock()
	//dLog.Debug(dLog.DLock, "%v Unlock", valueWithRWMutex.varName)
}

func getElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(rand.Intn(150)+600)
}

type Log struct {
	LogEntrySlice             []LogEntry
	AbsoluteIndexOfFirstEntry int
}

func (log *Log) getLogEntry(index int) (LogEntry, bool) {
	if index < len(log.LogEntrySlice) {
		return log.LogEntrySlice[index], true
	} else {
		return LogEntry{}, false
	}
}

func (log *Log) Last() LogEntry {
	return log.LogEntrySlice[len(log.LogEntrySlice)-1]
}

func (log *Log) append(logEntry LogEntry) {
	log.LogEntrySlice = append(log.LogEntrySlice, logEntry)
}

func (log *Log) locOfFirstEntryGivenTerm(term int) (int, bool) {
	index := sort.Search(len(log.LogEntrySlice), func(i int) bool {
		return log.LogEntrySlice[i].Term >= term
	})
	if index < len(log.LogEntrySlice) && log.LogEntrySlice[index].Term == term {
		return index, true
	} else {
		return -1, false
	}
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (logEntry LogEntry) String() string {
	return fmt.Sprintf("{Term: %v, Command: %v}",
		logEntry.Term, logEntry.Command)
}

// snapshot
type Snapshot struct {
	snapshot                  []byte
	snapshotLastIncludedIndex int
	snapshotLastIncludedTerm  int
}

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

// return currentTerm and whether this server
// believes it is the leader.
func (raft *Raft) GetState() (int, bool) {
	dLog.Debug(dLog.DLock, "Server %v is waiting for the RLock for currentTerm1 in GetState", raft.me)
	raft.currentTerm1.rwMutex.RLock()
	defer raft.currentTerm1.rwMutex.RUnlock()

	dLog.Debug(dLog.DLock, "Server %v is waiting for the RLock for role4 in GetState", raft.me)
	raft.role4.rwMutex.RLock()
	defer raft.role4.rwMutex.RUnlock()

	dLog.Debug(dLog.DInfo, "Server: %v, term: %v, role: %v",
		raft.me, raft.currentTerm1.Value, raft.role4.Value)
	return int(raft.currentTerm1.Value), raft.role4.Value == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (raft *Raft) persist(currentTerm int, votedFor int, log_ Log) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	err := e.Encode(currentTerm)
	if err != nil {
		log.Fatalf("persist: encoding error %v", err)
	}
	err = e.Encode(votedFor)
	if err != nil {
		log.Fatalf("persist: encoding error %v", err)
	}
	err = e.Encode(log_)
	if err != nil {
		log.Fatalf("persist: enconding error %v", err)
	}

	data := w.Bytes()
	raft.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (raft *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		dLog.Debug(dLog.DPersist, "readPersist: empty data")
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term int
	err := d.Decode(&term)
	if err != nil {
		log.Fatalf("readPersist: decoding error %v", err)
	}

	var votedFor int
	err = d.Decode(&votedFor)
	if err != nil {
		log.Fatalf("readPersist: decoding error %v", err)
	}

	var log_ Log
	err = d.Decode(&log_)
	if err != nil {
		log.Fatalf("readPersist: decoding error %v", err)
	}

	raft.currentTerm1.Lock()
	defer raft.currentTerm1.Unlock()
	raft.votedFor2.Lock()
	defer raft.votedFor2.Unlock()
	raft.log3.Lock()
	defer raft.log3.Unlock()

	raft.currentTerm1.Value = term
	raft.votedFor2.Value = votedFor
	raft.log3.Value = log_
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (raft *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (raft *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	data              []byte // raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (raft *Raft) SendInstallSnapshotRequest(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	dLog.Debug(dLog.DSnap, "Server %v is sending an installSnapshotRequest to %v", raft.me, server)
	ok := raft.peers[server].Call("Raft.ProcessInstallSnapshot", args, reply)
	if ok {
		dLog.Debug(dLog.DSnap, "Server %v gets a reply for installSnapshotRequest from %v", raft.me, server)
	} else {
		dLog.Debug(dLog.DSnap, "Server %v does NOT get a reply for installSnapshotRequest from %v", raft.me, server)
	}
	return ok
}

func (raft *Raft) ProcessInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	raft.convertToFollowerGivenLargerTerm(args.Term)

	// Reply immediately if term < currentTerm
	raft.currentTerm1.rwMutex.RLock()
	defer raft.currentTerm1.rwMutex.RUnlock()
	currentTerm := raft.currentTerm1.Value
	if args.Term < currentTerm {
		reply.Term = currentTerm
		return
	}

	// If existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	raft.log3.rwMutex.Lock()
	defer raft.log3.rwMutex.Unlock()
	relativeLastIncludedIndex := args.LastIncludedIndex - raft.log3.Value.AbsoluteIndexOfFirstEntry
	if relativeLastIncludedIndex < len(raft.log3.Value.LogEntrySlice) &&
		raft.log3.Value.LogEntrySlice[relativeLastIncludedIndex].Term == args.LastIncludedTerm {
		raft.snapshot12.rwMutex.Lock()
		defer raft.snapshot12.rwMutex.Unlock()
		raft.snapshot12.Value.snapshot = args.data
		raft.snapshot12.Value.snapshotLastIncludedIndex = args.LastIncludedIndex
		raft.snapshot12.Value.snapshotLastIncludedTerm = args.LastIncludedTerm

		raft.log3.Value.LogEntrySlice = raft.log3.Value.LogEntrySlice[relativeLastIncludedIndex+1:]
		reply.Term = raft.currentTerm1.Value
		return
	}

	// Discard the entire log
	raft.log3.Value.AbsoluteIndexOfFirstEntry = args.LastIncludedIndex + 1
	raft.log3.Value.LogEntrySlice = make([]LogEntry, 0)

	// TODO: Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	raft.snapshot12.rwMutex.Lock()
	defer raft.snapshot12.rwMutex.Unlock()
	raft.snapshot12.Value.snapshot = args.data
	raft.snapshot12.Value.snapshotLastIncludedIndex = args.LastIncludedIndex
	raft.snapshot12.Value.snapshotLastIncludedTerm = args.LastIncludedTerm
}

// Invoked by leader to replicate log entries (§5.3); also used as
// heartbeat (§5.2).
type AppendEntriesArgs struct {
	Term            int        // leader’s value
	LeaderId        int        // so follower can redirect clients
	PrevLogIndex    int        // index of log entry immediately preceding new ones
	PrevLogTerm     int        // value of prevLogIndex entry
	Entries         []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIdx int        // leader’s commitIndex
}

func (appendEntriesArgs AppendEntriesArgs) String() string {
	entriesStringBuilder := strings.Builder{}
	for idx, entry := range appendEntriesArgs.Entries {
		entriesStringBuilder.WriteString(entry.String())
		if idx != len(appendEntriesArgs.Entries)-1 {
			entriesStringBuilder.WriteString(", ")
		}
	}
	return fmt.Sprintf(
		"{Term: %v, LeaderId: %v, "+
			"PrevLogIndex: %v, PrevLogTerm: %v, "+
			"LeaderCommitIdx: %v, Entries: %v}",
		appendEntriesArgs.Term, appendEntriesArgs.LeaderId,
		appendEntriesArgs.PrevLogIndex, appendEntriesArgs.PrevLogTerm,
		appendEntriesArgs.LeaderCommitIdx, entriesStringBuilder.String())
}

type AppendEntriesReply struct {
	Term                                       int  // currentTerm, for leader to update itself
	Success                                    bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictingTerm                            int  // the term of the conflicting entry
	IndexOfTheFirstEntryWithTheConflictingTerm int  // the index of the first entry with the conflicting term
}

func (appendEntriesReply AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term: %v, Success: %v}",
		appendEntriesReply.Term, appendEntriesReply.Success)
}

func (raft *Raft) SendAppendEntriesRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	dLog.Debug(dLog.DAppend,
		"Server %v is sending an appendEntriesRequest to %v with %v entries",
		raft.me, server, len(args.Entries))
	ok := raft.peers[server].Call("Raft.ProcessAppendEntries", args, reply)
	if ok {
		dLog.Debug(dLog.DAppend,
			"Server %v gets a reply for appendEntriesRequest from %v",
			raft.me, server)
	} else {
		dLog.Debug(dLog.DAppend,
			"Server %v does NOT get a reply for appendEntriesRequest from %v",
			raft.me, server)
	}
	return ok
}

const INVALID_TERM int = -1

func (raft *Raft) ProcessAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	dLog.Debug(dLog.DAppend,
		"Server %v received an appendEntriesRequest %v",
		raft.me, args.String())

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	raft.convertToFollowerGivenLargerTerm(args.Term)

	// hold the read lock of currentTerm, so it is consistent and cannot change during the process
	raft.currentTerm1.rwMutex.RLock()
	defer raft.currentTerm1.rwMutex.RUnlock()
	currentTerm := raft.currentTerm1.Value
	// Reply false if value < currentTerm (§5.1)
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		dLog.Debug(dLog.DAppend,
			"Server %v refuses the appendEntriesRequest from the server %v "+
				"because the invalid term: %v < %v",
			raft.me, args.LeaderId, args.Term, currentTerm)
		return
	}

	// set timeout flag
	// you get an AppendEntries RPC from the current leader
	// (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer)
	//  If election timeout elapses without receiving AppendEntries
	// RPC from current leader or granting vote to candidate:
	// convert to candidate
	raft.electionTimer10.Reset(getElectionTimeout())
	dLog.Debug(dLog.DTimer, "Server %v resets the election timer", raft.me)

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	raft.log3.rwMutex.Lock()
	defer raft.log3.rwMutex.Unlock()
	relativeArgsPrevLogIndex := args.PrevLogIndex - raft.log3.Value.AbsoluteIndexOfFirstEntry
	if relativeArgsPrevLogIndex >= len(raft.log3.Value.LogEntrySlice) {
		reply.Success = false
		reply.Term = currentTerm
		reply.ConflictingTerm = INVALID_TERM
		reply.IndexOfTheFirstEntryWithTheConflictingTerm = len(raft.log3.Value.LogEntrySlice)
		dLog.Debug(dLog.DAppend,
			"Server %v refuses the appendEntriesRequest from server %v because "+
				"log doesn’t contain an entry at prevLogIndex %v so it returns the next index %v of the last entry",
			raft.me, args.LeaderId, args.PrevLogIndex, reply.IndexOfTheFirstEntryWithTheConflictingTerm)
		return
	} else {
		logEntry := raft.log3.Value.LogEntrySlice[relativeArgsPrevLogIndex]
		if logEntry.Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = currentTerm
			reply.ConflictingTerm = logEntry.Term
			locOfFirstEntryWithConflictingTerm, found := raft.log3.Value.locOfFirstEntryGivenTerm(reply.ConflictingTerm)
			if !found { // it must and can find
				log.Fatalf("Server %v cannot find the first entry with term %v", raft.me, reply.ConflictingTerm)
			}
			reply.IndexOfTheFirstEntryWithTheConflictingTerm = locOfFirstEntryWithConflictingTerm
			dLog.Debug(dLog.DAppend,
				"Server %v refuses the appendEntriesRequest from server %v because "+
					"log doesn’t contain an entry at prevLogIndex %v whose term matches prevLogTerm %v, "+
					"so it returns the next index %v of the first entry with the conflicting term %v",
				raft.me, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
				reply.IndexOfTheFirstEntryWithTheConflictingTerm, reply.ConflictingTerm)
			return
		}
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// assume the args.Entries are sorted in increasing order
	for remoteLogEntryIdx, remoteLogEntry := range args.Entries {
		absoluteLoc := args.PrevLogIndex + remoteLogEntryIdx + 1
		relativeLoc := absoluteLoc - raft.log3.Value.AbsoluteIndexOfFirstEntry
		if relativeLoc < len(raft.log3.Value.LogEntrySlice) {
			if raft.log3.Value.LogEntrySlice[relativeLoc].Term != remoteLogEntry.Term {
				dLog.Debug(dLog.DAppend,
					"Server %v delete the entries starting from %v "+
						"because the conflict of existing entry term %v with new entry term %v at index %v",
					raft.me, absoluteLoc, raft.log3.Value.LogEntrySlice[relativeLoc].Term, remoteLogEntry.Term, relativeLoc)
				raft.log3.Value.LogEntrySlice = raft.log3.Value.LogEntrySlice[:relativeLoc]
				break
			}
		} else {
			break
		}
	}

	// Append any new entries not already in the log
	// assume the args.Entries are sorted in increasing order
	for remoteLogEntryIndex, remoteLogEntry := range args.Entries {
		absoluteLoc := args.PrevLogIndex + remoteLogEntryIndex + 1
		relativeLoc := absoluteLoc - raft.log3.Value.AbsoluteIndexOfFirstEntry
		if relativeLoc >= len(raft.log3.Value.LogEntrySlice) {
			raft.log3.Value.append(remoteLogEntry)
			dLog.Debug(dLog.DAppend,
				"Server %v append a new entry %v at %v that is not already in the log",
				raft.me, remoteLogEntry.String(), relativeLoc)
		}
	}
	raft.persist(raft.currentTerm1.Value, raft.votedFor2.Value, raft.log3.Value)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	raft.commitIndex6.rwMutex.Lock()
	defer raft.commitIndex6.rwMutex.Unlock()
	if args.LeaderCommitIdx > raft.commitIndex6.Value {
		//The min in the final step (#5) of AppendEntries is necessary,
		//and it needs to be computed with the index of the last new entry.
		//It is not sufficient to simply have the function that applies things
		//from your log between lastApplied and commitIndex stop when it reaches the end of your log.
		//This is because you may have entries in your log that differ from the leader’s log after the entries that
		//the leader sent you (which all match the ones in your log).
		//Because #3 dictates that you only truncate your log if you have conflicting entries, those won’t be removed,
		//and if leaderCommit is beyond the entries the leader sent you, you may apply incorrect entries.
		dLog.Debug(dLog.DAppend,
			"Server %v update the commitIndex from %v to %v, "+
				"leaderCommitIndex: %v, index of last new entry: %v",
			raft.me, raft.commitIndex6.Value, min(args.LeaderCommitIdx, args.PrevLogIndex+len(args.Entries)),
			args.LeaderCommitIdx, args.PrevLogIndex+len(args.Entries))
		raft.commitIndex6.Value = min(args.LeaderCommitIdx, args.PrevLogIndex+len(args.Entries))
		go raft.applyCommittedCommand()
	}
	reply.Success = true
	reply.Term = currentTerm
	return
}

// example ProcessRequestVoteRequest RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's value
	CandidateID  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // value of candidate’s last log entry (§5.4)
}

// example ProcessRequestVoteRequest RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //current value, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example code to send a ProcessRequestVoteRequest RPC to a server.
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
func (raft *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	dLog.Debug(dLog.DVote, "Server %v requests a vote from %v", raft.me, server)
	ok := raft.peers[server].Call("Raft.ProcessRequestVoteRequest", args, reply)
	return ok
}

// example ProcessRequestVoteRequest RPC handler.
func (raft *Raft) ProcessRequestVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower (§5.1)
	// if you have already voted in the current term, and an incoming RequestVote RPC has a higher term that you,
	// you should first step down and adopt their term (thereby resetting votedFor),
	// and then handle the RPC, which will result in you granting the vote!
	raft.convertToFollowerGivenLargerTerm(args.Term)

	// Your code here (2A, 2B).
	// hold the read lock of currentTerm, so it is consistent and cannot change during the process
	raft.currentTerm1.rwMutex.RLock()
	defer raft.currentTerm1.rwMutex.RUnlock()
	currentTerm := raft.currentTerm1.Value
	if args.Term < currentTerm {
		reply.VoteGranted = false
		reply.Term = currentTerm
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	raft.votedFor2.rwMutex.Lock()
	defer raft.votedFor2.rwMutex.Unlock()
	raft.log3.rwMutex.RLock()
	defer raft.log3.rwMutex.RUnlock()
	if (raft.votedFor2.Value == VOTED_FOR_NO_ONE || raft.votedFor2.Value == args.CandidateID) &&
		(args.LastLogTerm > raft.log3.Value.Last().Term ||
			(args.LastLogTerm == raft.log3.Value.Last().Term &&
				args.LastLogIndex >= raft.log3.Value.AbsoluteIndexOfFirstEntry+len(raft.log3.Value.LogEntrySlice)-1)) {
		dLog.Debug(dLog.DVote, "Server %v grants a vote to %v", raft.me, args.CandidateID)
		reply.VoteGranted = true
		raft.votedFor2.Value = args.CandidateID
		reply.Term = currentTerm
		raft.persist(raft.currentTerm1.Value, raft.votedFor2.Value, raft.log3.Value)
		// restart your election timer if you grant a vote to another peer.
		// If election timeout elapses without receiving AppendEntries
		// RPC from current leader or granting vote to candidate:
		// convert to candidate
		raft.electionTimer10.Reset(getElectionTimeout())
	} else {
		reply.VoteGranted = false
		reply.Term = currentTerm
	}
	return
}

// TODO: make it less ugly
func (raft *Raft) convertToFollowerGivenLargerTerm(term int) bool {
	dLog.Debug(dLog.DLock,
		"Server %v is waiting for the Lock for currentTerm1 in convertToFollowerGivenLargerTerm", raft.me)
	raft.currentTerm1.rwMutex.Lock()
	defer raft.currentTerm1.Unlock()
	dLog.Debug(dLog.DLock,
		"Server %v obtained the Lock for currentTerm1 in convertToFollowerGivenLargerTerm", raft.me)

	if term > raft.currentTerm1.Value {
		dLog.Debug(dLog.DLock,
			"Server %v is waiting for the Lock for votedFor2 in convertToFollowerGivenLargerTerm", raft.me)
		raft.votedFor2.rwMutex.Lock()
		defer raft.votedFor2.Unlock()

		dLog.Debug(dLog.DLock,
			"Server %v is waiting for the RLock of log3 in convertToFollowerGivenLargerTerm", raft.me)
		raft.log3.rwMutex.RLock()
		defer raft.log3.rwMutex.RUnlock()

		dLog.Debug(dLog.DLock,
			"Server %v is waiting for the Lock for role4 in convertToFollowerGivenLargerTerm", raft.me)
		raft.role4.rwMutex.Lock()
		defer raft.role4.Unlock()

		dLog.Debug(dLog.DTerm,
			"Server %v converts to follower and update term from %v to %v",
			raft.me, raft.currentTerm1.Value, term)

		raft.currentTerm1.Value = term
		raft.votedFor2.Value = VOTED_FOR_NO_ONE
		raft.role4.Value = FOLLOWER
		select {
		case <-raft.roleChannel5:
			break
		default:
			break
		}
		raft.persist(raft.currentTerm1.Value, raft.votedFor2.Value, raft.log3.Value)
		return true
	} else {
		return false
	}
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
// if it's ever committed.
// the second return value is the current term.
// the third return value is true if this server believes it is the leader.
func (raft *Raft) Start(command interface{}) (int, int, bool) {
	if raft.killed() == true {
		index := -1
		term := -1
		return index, term, false
	}

	raft.currentTerm1.rwMutex.RLock()
	defer raft.currentTerm1.rwMutex.RUnlock()
	raft.log3.rwMutex.Lock()
	defer raft.log3.rwMutex.Unlock()
	raft.role4.rwMutex.RLock()
	defer raft.role4.rwMutex.RUnlock()

	if raft.role4.Value != LEADER {
		index := -1
		term := -1
		return index, term, false
	}
	dLog.Debug(dLog.DCommit, "Server %v receives a command %v to be committed",
		raft.me, command)

	potentialCommittedIndex := raft.log3.Value.AbsoluteIndexOfFirstEntry + len(raft.log3.Value.LogEntrySlice)

	// If command received from client: append entry to local log,
	// respond after entry applied to state machine (§5.3)
	raft.log3.Value.append(LogEntry{
		Term:    raft.currentTerm1.Value,
		Command: command,
	})
	raft.persist(raft.currentTerm1.Value, raft.votedFor2.Value, raft.log3.Value)
	go raft.synchronizeLog()
	return potentialCommittedIndex, int(raft.currentTerm1.Value), true
}

func (raft *Raft) synchronizeLog() {
	// the following tasks could be proposal-driven or be put into a single periodically executed coroutine
	// maybe periodically executed coroutine is better because that will decouple the 2 tasks and break the causal relationship between them
	// If last log index ≥ nextIndex for a follower:
	// send AppendEntries RPC with log entries starting at nextIndex
	raft.currentTerm1.rwMutex.RLock()
	raft.log3.rwMutex.RLock()
	raft.role4.rwMutex.RLock()
	if raft.role4.Value == LEADER {
		raft.commitIndex6.rwMutex.RLock()
		for peerIdx, _ := range raft.peers {
			raft.nextIndexSlice8[peerIdx].rwMutex.RLock()
			if peerIdx != raft.me {
				var logEntries []LogEntry
				relativeNextIndex := raft.nextIndexSlice8[peerIdx].Value - raft.log3.Value.AbsoluteIndexOfFirstEntry
				if len(raft.log3.Value.LogEntrySlice)-1 >= relativeNextIndex {
					logEntries = raft.log3.Value.LogEntrySlice[relativeNextIndex:]
				} else {
					logEntries = []LogEntry{}
				}
				prevLogEntryIndex := raft.nextIndexSlice8[peerIdx].Value - 1
				prevLogEntry := raft.log3.Value.LogEntrySlice[prevLogEntryIndex-raft.log3.Value.AbsoluteIndexOfFirstEntry]
				appendEntriesArgs := AppendEntriesArgs{
					Term:            raft.currentTerm1.Value,
					LeaderId:        raft.me,
					PrevLogIndex:    prevLogEntryIndex,
					PrevLogTerm:     prevLogEntry.Term,
					Entries:         logEntries,
					LeaderCommitIdx: raft.commitIndex6.Value,
				}
				go func(peerIdx int, appendEntriesArgs AppendEntriesArgs) {
					raft.trySendingAppendEntriesTo(peerIdx, appendEntriesArgs)
				}(peerIdx, appendEntriesArgs)
			}
			raft.nextIndexSlice8[peerIdx].rwMutex.RUnlock()
		}
		raft.commitIndex6.rwMutex.RUnlock()
	}
	raft.role4.rwMutex.RUnlock()
	raft.log3.rwMutex.RUnlock()
	raft.currentTerm1.rwMutex.RUnlock()

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	raft.currentTerm1.rwMutex.RLock()
	raft.log3.rwMutex.RLock()
	raft.commitIndex6.rwMutex.Lock()
	maxOfMatchIndex := math.MinInt
	for idx := range raft.matchIndexSlice9 {
		raft.matchIndexSlice9[idx].rwMutex.RLock()
		curMatchIndex := raft.matchIndexSlice9[idx].Value
		if curMatchIndex > maxOfMatchIndex {
			maxOfMatchIndex = curMatchIndex
		}
		raft.matchIndexSlice9[idx].rwMutex.RUnlock()
	}
	for newCommitIndex := raft.commitIndex6.Value + 1; newCommitIndex <= maxOfMatchIndex; newCommitIndex += 1 {
		largerMatchCount := 1 // the leader matches itself for all its log
		for idx, _ := range raft.matchIndexSlice9 {
			raft.matchIndexSlice9[idx].rwMutex.RLock()
			if raft.matchIndexSlice9[idx].Value >= newCommitIndex {
				largerMatchCount += 1
			}
			raft.matchIndexSlice9[idx].rwMutex.RUnlock()
		}

		if largerMatchCount > len(raft.peers)/2 {
			if newCommitIndex < len(raft.log3.Value.LogEntrySlice) {
				logEntry := raft.log3.Value.LogEntrySlice[newCommitIndex-raft.log3.Value.AbsoluteIndexOfFirstEntry]
				if logEntry.Term == raft.currentTerm1.Value {
					// commit
					dLog.Debug(dLog.DCommit,
						"Server %v update commitIndex from %v to %v "+
							"because the majority agrees on the entry at index of %v",
						raft.me, raft.commitIndex6.Value, newCommitIndex,
						newCommitIndex)
					raft.commitIndex6.Value = newCommitIndex
					go raft.applyCommittedCommand()
				}
			}
		}
	}
	raft.commitIndex6.rwMutex.Unlock()
	raft.log3.rwMutex.RUnlock()
	raft.currentTerm1.rwMutex.RUnlock()
}

func (raft *Raft) trySendingAppendEntriesTo(peerIdx int, appendEntriesArgs AppendEntriesArgs) bool {
	appendEntriesReply := AppendEntriesReply{}
	ok := raft.SendAppendEntriesRequest(peerIdx, &appendEntriesArgs, &appendEntriesReply)
	if ok {
		isLarger := raft.convertToFollowerGivenLargerTerm(appendEntriesReply.Term)
		if !isLarger {
			dLog.Debug(dLog.DLock,
				"Server %v is waiting for the lock for currentTerm1 in trySendingAppendEntriesTo",
				raft.me)
			raft.currentTerm1.rwMutex.RLock()
			dLog.Debug(dLog.DLock,
				"Server %v obtained the lock for currentTerm1 in trySendingAppendEntriesTo",
				raft.me)

			currentTerm := raft.currentTerm1.Value
			raft.currentTerm1.rwMutex.RUnlock()

			if appendEntriesArgs.Term == currentTerm {
				if appendEntriesReply.Success {
					//If successful: update nextIndex and matchIndex for follower (§5.3)
					//A related, but not identical problem is that of assuming that your state has not changed
					//between when you sent the RPC, and when you received the reply.
					//A good example of this is setting matchIndex = nextIndex - 1, or matchIndex = len(log)
					//when you receive a response to an RPC. This is not safe, because both of those values
					//could have been updated since when you sent the RPC.
					//Instead, the correct thing to do is update matchIndex to be prevLogIndex + len(entries[])
					//from the arguments you sent in the RPC originally.
					newMatchIndex := appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries)

					raft.nextIndexSlice8[peerIdx].rwMutex.Lock()
					raft.matchIndexSlice9[peerIdx].rwMutex.Lock()

					//matchIndex is initialized to 0, increases monotonically
					if newMatchIndex > raft.matchIndexSlice9[peerIdx].Value {
						dLog.Debug(dLog.DAppend,
							"Server %v receives a reply from server %v, "+
								"update the corresponding matchIndex from %v to %v, and "+
								"update the corresponding nextIndex from %v to %v",
							raft.me, peerIdx,
							raft.matchIndexSlice9[peerIdx].Value, newMatchIndex,
							raft.nextIndexSlice8[peerIdx].Value, newMatchIndex+1)

						raft.matchIndexSlice9[peerIdx].Value = newMatchIndex
						raft.nextIndexSlice8[peerIdx].Value = newMatchIndex + 1
					}

					raft.matchIndexSlice9[peerIdx].rwMutex.Unlock()
					raft.nextIndexSlice8[peerIdx].rwMutex.Unlock()

					return true
				} else {
					//If AppendEntries fails because of log inconsistency:
					//decrement nextIndex and retry (§5.3)
					raft.log3.rwMutex.RLock()
					raft.nextIndexSlice8[peerIdx].rwMutex.Lock()

					var newAppendEntriesArgs = AppendEntriesArgs{}
					// only retry if the nextIndex doesn't change
					if appendEntriesArgs.PrevLogIndex+1 == raft.nextIndexSlice8[peerIdx].Value {
						oldNextIndex := raft.nextIndexSlice8[peerIdx].Value
						// TODO: Upon receiving a conflict response,
						// the leader should first search its log for conflictTerm.
						// If it finds an entry in its log with that term,
						// it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
						// If it does not find an entry with that term, it should set nextIndex = conflictIndex.
						raft.nextIndexSlice8[peerIdx].Value = appendEntriesReply.IndexOfTheFirstEntryWithTheConflictingTerm
						dLog.Debug(dLog.DAppend,
							"Server %v fails an appendEntries for server %v,  "+
								"decrement nextIndex from %v to %v, and retry",
							raft.me, peerIdx,
							oldNextIndex, raft.nextIndexSlice8[peerIdx].Value)

						prevLogEntryIndex := raft.nextIndexSlice8[peerIdx].Value - 1
						relativePrevLogEntryIndex := prevLogEntryIndex - raft.log3.Value.AbsoluteIndexOfFirstEntry
						if relativePrevLogEntryIndex < len(raft.log3.Value.LogEntrySlice) {
							prevLogEntry := raft.log3.Value.LogEntrySlice[relativePrevLogEntryIndex]
							newAppendEntriesArgs = AppendEntriesArgs{
								Term:         appendEntriesArgs.Term,
								LeaderId:     raft.me,
								PrevLogIndex: prevLogEntryIndex,
								PrevLogTerm:  prevLogEntry.Term,
								// send the entries between nextIndex and the last of old entries
								Entries:         raft.log3.Value.LogEntrySlice[raft.nextIndexSlice8[peerIdx].Value-raft.log3.Value.AbsoluteIndexOfFirstEntry : appendEntriesArgs.PrevLogIndex+len(appendEntriesArgs.Entries)+1-raft.log3.Value.AbsoluteIndexOfFirstEntry],
								LeaderCommitIdx: appendEntriesArgs.LeaderCommitIdx,
							}
						} else {
							log.Fatal("not found")
						}
					}

					raft.nextIndexSlice8[peerIdx].rwMutex.Unlock()
					raft.log3.rwMutex.RUnlock()

					if len(newAppendEntriesArgs.Entries) > 0 {
						return raft.trySendingAppendEntriesTo(peerIdx, newAppendEntriesArgs)
					}
				}
			}
		}
	}
	return false
}

func (raft *Raft) applyCommittedCommand() {
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	// TODO: is applyChannel guaranteed to be safe?
	raft.log3.rwMutex.RLock()
	defer raft.log3.rwMutex.RUnlock()
	raft.commitIndex6.rwMutex.RLock()
	defer raft.commitIndex6.rwMutex.RUnlock()
	raft.lastApplied7.rwMutex.Lock()
	defer raft.lastApplied7.rwMutex.Unlock()
	for raft.commitIndex6.Value > raft.lastApplied7.Value {
		raft.lastApplied7.Value += 1
		raft.applyChannel11 <- ApplyMsg{
			CommandValid:  true,
			Command:       raft.log3.Value.LogEntrySlice[raft.lastApplied7.Value-raft.log3.Value.AbsoluteIndexOfFirstEntry].Command,
			CommandIndex:  raft.lastApplied7.Value,
			SnapshotValid: false, //todo: update those none values
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
		dLog.Debug(dLog.DCommit,
			"Server %v applied the command %v at index %v",
			raft.me, raft.log3.Value.LogEntrySlice[raft.lastApplied7.Value-raft.log3.Value.AbsoluteIndexOfFirstEntry].Command, raft.lastApplied7.Value)
	}
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
func (raft *Raft) Kill() {
	atomic.StoreInt32(&raft.dead, DEAD)
	dLog.Debug(dLog.DInfo, "Server %v is killed", raft.me)
}

// TODO: call killed in all goroutines to avoid printing confusing messages
func (raft *Raft) killed() bool {
	z := atomic.LoadInt32(&raft.dead)
	return z == DEAD
}

// The ticker go routine starts a new election if this peer hasn't received heartbeats recently.
// If election timeout elapses without receiving AppendEntries RPC from current leader or granting vote to candidate:
// convert to candidate
func (raft *Raft) ticker() {
	for raft.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using time.Sleep().
		dLog.Debug(dLog.DTimer, "Server %v is waiting for election timer", raft.me)
		<-raft.electionTimer10.C
		dLog.Debug(dLog.DTimer, "Server %v finds its election timer timeout", raft.me)

		if raft.killed() == false {
			raft.role4.rwMutex.RLock() // TODO: we can't wait for lock here because the timer may get reset before getting the lock. Rewrite the electionTimer with time.sleep and a variable recording the lastTimeCheckedIn
			role := raft.role4.Value
			raft.role4.rwMutex.RUnlock()

			if role != LEADER { // TODO: verify if it's required to check the role
				raft.startElection()
			}
		}
	}
}

// Candidates (§5.2):
func (raft *Raft) startElection() {
	dLog.Debug(dLog.DLock,
		"Server %v is waiting for the Lock for currentTerm1 in startElection", raft.me)
	raft.currentTerm1.rwMutex.Lock()

	dLog.Debug(dLog.DLock,
		"Server %v is waiting for the Lock for votedFor2 in startElection", raft.me)
	raft.votedFor2.rwMutex.Lock()

	dLog.Debug(dLog.DLock,
		"Server %v is waiting for the Lock for log3 in startElection", raft.me)
	raft.log3.rwMutex.RLock()

	dLog.Debug(dLog.DLock,
		"Server %v is waiting for the Lock for role4 in startElection", raft.me)
	raft.role4.rwMutex.Lock()

	// On conversion to candidate, start election:
	raft.role4.Value = CANDIDATE

	// • Increment currentTerm
	raft.currentTerm1.Value += 1
	dLog.Debug(dLog.DVote, "Server %v starts an election with a term %v", raft.me, raft.currentTerm1.Value)

	// • Vote for self
	voteChannel := make(chan bool, len(raft.peers))
	voteChannel <- true
	raft.votedFor2.Value = raft.me

	// • Reset election timer
	raft.electionTimer10.Reset(getElectionTimeout())

	raft.persist(raft.currentTerm1.Value, raft.votedFor2.Value, raft.log3.Value)

	// • Send ProcessRequestVoteRequest RPCs to all other servers
	for peerIdx := range raft.peers {
		if peerIdx != raft.me {
			requestVoteArgs := RequestVoteArgs{
				Term:         raft.currentTerm1.Value,
				CandidateID:  raft.me,
				LastLogIndex: raft.log3.Value.AbsoluteIndexOfFirstEntry + len(raft.log3.Value.LogEntrySlice) - 1,
				LastLogTerm:  raft.log3.Value.Last().Term,
			}
			go func(peerIdx int, requestVoteArgs RequestVoteArgs) {
				requestVoteReply := RequestVoteReply{}
				ok := raft.sendRequestVote(peerIdx, &requestVoteArgs, &requestVoteReply)
				if ok {
					isLarger := raft.convertToFollowerGivenLargerTerm(requestVoteReply.Term)
					if !isLarger {
						raft.currentTerm1.rwMutex.RLock()
						currentTerm := raft.currentTerm1.Value
						raft.currentTerm1.rwMutex.RUnlock()
						if requestVoteArgs.Term == currentTerm {
							if requestVoteReply.VoteGranted {
								dLog.Debug(dLog.DVote, "Server %v receives a vote from %v", raft.me, peerIdx)
								voteChannel <- true
							}
						}
					}
				}
			}(peerIdx, requestVoteArgs)
		}
	}
	raft.role4.rwMutex.Unlock()
	raft.log3.rwMutex.RUnlock()
	raft.votedFor2.rwMutex.Unlock()
	raft.currentTerm1.rwMutex.Unlock()

	// If votes received from the majority of servers: become leader
	// todo: If AppendEntries RPC received from new leader: convert to follower
	// If election timeout elapses: start new election
	voteSum := 0
	for raft.killed() == false {
		raft.role4.rwMutex.RLock()
		role := raft.role4.Value
		raft.role4.rwMutex.RUnlock()
		if role == CANDIDATE {
			select {
			case <-voteChannel:
				voteSum += 1
				if voteSum > len(raft.peers)/2 {
					raft.convertToLeader()
					dLog.Debug(dLog.DLeader, "Server %v converted to a leader", raft.me)
					return
				}
			case raft.roleChannel5 <- role:
				dLog.Debug(dLog.DVote, "Server %v has already converted to follower", raft.me)
				raft.electionTimer10.Reset(getElectionTimeout())
				return
			case <-raft.electionTimer10.C:
				dLog.Debug(dLog.DVote, "Server %v starts another election", raft.me)
				raft.startElection()
				return
			}
		}
	}
	return
}

const HEARTBEAT_TIMEOUT = time.Millisecond * 100

func (raft *Raft) convertToLeader() {
	dLog.Debug(dLog.DLock,
		"Server %v is waiting for the lock for log3 in convertToLeader",
		raft.me)
	raft.log3.rwMutex.RLock()
	defer raft.log3.rwMutex.RUnlock()

	dLog.Debug(dLog.DLock,
		"Server %v is waiting for the lock for role4 in convertToLeader",
		raft.me)
	raft.role4.rwMutex.Lock()
	defer raft.role4.rwMutex.Unlock()

	raft.role4.Value = LEADER

	//reinitialize
	for idx, _ := range raft.nextIndexSlice8 {
		raft.nextIndexSlice8[idx].rwMutex.Lock()
		raft.nextIndexSlice8[idx].Value = raft.log3.Value.AbsoluteIndexOfFirstEntry + len(raft.log3.Value.LogEntrySlice)
		raft.nextIndexSlice8[idx].rwMutex.Unlock()
	}

	for idx, _ := range raft.matchIndexSlice9 {
		raft.matchIndexSlice9[idx].rwMutex.Lock()
		raft.matchIndexSlice9[idx].Value = 0
		raft.matchIndexSlice9[idx].rwMutex.Unlock()
	}

	//Upon election: send initial empty AppendEntries RPCs
	//(heartbeat) to each server; repeat during idle periods to
	//prevent election timeouts (§5.2)
	go raft.sendHeartBeat()
}

func (raft *Raft) sendHeartBeat() {
	heartbeatTicker := time.NewTicker(HEARTBEAT_TIMEOUT)
	defer heartbeatTicker.Stop()
	for raft.killed() == false {
		<-heartbeatTicker.C
		if raft.killed() == false {
			raft.role4.rwMutex.RLock()
			role := raft.role4.Value
			raft.role4.rwMutex.RUnlock()

			if role == LEADER {
				raft.synchronizeLog()
			}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	dLog.Debug(dLog.DPersist, "Server %v starts and initializes", me)

	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      LIVE,
		currentTerm1: ValueWithRWMutex[int]{
			varName: "currentTerm1",
			Value:   0,
			rwMutex: sync.RWMutex{},
		},
		votedFor2: ValueWithRWMutex[int]{
			varName: "votedFor2",
			Value:   VOTED_FOR_NO_ONE,
			rwMutex: sync.RWMutex{},
		},
		log3: ValueWithRWMutex[Log]{
			varName: "log3",
			Value: Log{
				LogEntrySlice: []LogEntry{
					{
						Term:    0,
						Command: nil,
					},
				},
				AbsoluteIndexOfFirstEntry: 0,
			},
			rwMutex: sync.RWMutex{},
		},
		role4: ValueWithRWMutex[int]{
			varName: "role4",
			Value:   FOLLOWER,
			rwMutex: sync.RWMutex{},
		},
		roleChannel5: make(chan int),
		commitIndex6: ValueWithRWMutex[int]{
			varName: "commitIndex6",
			Value:   0,
			rwMutex: sync.RWMutex{},
		},
		lastApplied7: ValueWithRWMutex[int]{
			varName: "lastApplied7",
			Value:   0,
			rwMutex: sync.RWMutex{},
		},
		nextIndexSlice8:  make([]ValueWithRWMutex[int], len(peers)),
		matchIndexSlice9: make([]ValueWithRWMutex[int], len(peers)),
		electionTimer10:  time.NewTicker(getElectionTimeout()),
		applyChannel11:   applyCh,
	}
	sync.OnceFunc(func() {
		dLog.Init()
		dLog.IgnoreTopic(dLog.DLock)
	})()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
