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
	currentTerm1 ValueWithRWMutex[int64] // latest value sever has seen
	votedFor2    ValueWithRWMutex[int]   // candidateId that received vote in current value
	log3         ValueWithRWMutex[Log]   // log entries

	// volatile state on all servers
	role4        ValueWithRWMutex[int]   // role of the server
	roleChannel5 chan int                // channel for sending role
	commitIndex6 ValueWithRWMutex[int64] // index of highest log entry known to be committed
	lastApplied7 ValueWithRWMutex[int64] // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndexSlice8  []ValueWithRWMutex[int64] // for each server, index of the next log entry to send to that server
	matchIndexSlice9 []ValueWithRWMutex[int64] // for each server, index of highest log entry known to be replicated on server

	// follower
	//TODO: The management of the election timeout is a common source of
	//headaches. Perhaps the simplest plan is to maintain a variable in the
	//Raft struct containing the last time at which the peer heard from the
	//leader, and to have the election timeout goroutine periodically check
	//to see whether the time since then is greater than the timeout period.
	//It's easiest to use time.Sleep() with a small constant argument to
	//drive the periodic checks. Don't use time.Ticker and time.Timer;
	//they are tricky to use correctly.
	electionTimer10 *time.Timer

	// leader
	//todo: You'll want to have a separate long-running goroutine that sends
	//committed log entries in order on the applyCh. It must be separate,
	//since sending on the applyCh can block; and it must be a single
	//goroutine, since otherwise it may be hard to ensure that you send log
	//entries in log order. The code that advances commitIndex will need to
	//kick the apply goroutine; it's probably easiest to use a condition
	//variable (Go's sync.Cond) for this.
	applyChannel11 chan ApplyMsg
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

type ValueWithRWMutex[T any] struct {
	value   T
	rwMutex sync.RWMutex
}

func getElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(rand.Intn(150)+300)
}

type Log struct {
	logEntrySlice []LogEntry
}

func (log *Log) at(idx int) LogEntry {
	return log.logEntrySlice[idx]
}

func (log *Log) Last() LogEntry {
	return log.logEntrySlice[len(log.logEntrySlice)-1]
}

func (log *Log) FindLocationByEntryIndex(entryIndex int64) (int, bool) {
	return sort.Find(len(log.logEntrySlice), func(i int) int {
		return int(entryIndex - log.logEntrySlice[i].Index)
	})
}

func (log *Log) FindEntryByEntryIndex(entryIndex int64) (LogEntry, bool) {
	loc, found := log.FindLocationByEntryIndex(entryIndex)
	if !found {
		return LogEntry{}, false
	} else {
		return log.logEntrySlice[loc], true
	}
}

func (log *Log) subSlice(start int, end int) []LogEntry {
	return log.logEntrySlice[start:end]
}

func (log *Log) setLogEntrySlice(logEntrySlice []LogEntry) {
	log.logEntrySlice = logEntrySlice
}

func (log *Log) append(logEntry LogEntry) {
	log.logEntrySlice = append(log.logEntrySlice, logEntry)
}

type LogEntry struct {
	Term    int64       // value when entry was received by leader
	Index   int64       // index of the log entry
	Command interface{} // command for state machine
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
	raft.currentTerm1.rwMutex.RLock()
	defer raft.currentTerm1.rwMutex.RUnlock()
	raft.role4.rwMutex.RLock()
	defer raft.role4.rwMutex.RUnlock()
	return int(raft.currentTerm1.value), raft.role4.value == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (raft *Raft) persist() {
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
func (raft *Raft) readPersist(data []byte) {
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

// Invoked by leader to replicate log entries (§5.3); also used as
// heartbeat (§5.2).
type AppendEntriesArgs struct {
	Term            int64      // leader’s value
	LeaderId        int        // so follower can redirect clients
	PrevLogIndex    int64      // index of log entry immediately preceding new ones
	PrevLogTerm     int64      // value of prevLogIndex entry
	Entries         []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommitIdx int64      // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int64 // currentTerm, for leader to update itself
	Success bool  // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (raft *Raft) SendAppendEntriesRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := raft.peers[server].Call("Raft.ProcessAppendEntries", args, reply)
	return ok
}

func (raft *Raft) ProcessAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	raft.convertToFollowerGivenLargerTerm(args.Term)

	// hold the read lock of currentTerm, so it is consistent and cannot change during the process
	raft.currentTerm1.rwMutex.RLock()
	defer raft.currentTerm1.rwMutex.RUnlock()
	currentTerm := raft.currentTerm1.value
	// Reply false if value < currentTerm (§5.1)
	if args.Term < currentTerm {
		reply.Success = false
		reply.Term = currentTerm
		return
	}

	// set timeout flag
	// you get an AppendEntries RPC from the current leader
	// (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer)
	raft.electionTimer10.Reset(getElectionTimeout())

	// Reply false if log doesn’t contain an entry at prevLogIndex whose value matches prevLogTerm (§5.3)
	raft.log3.rwMutex.RLock()
	logEntry, found := raft.log3.value.FindEntryByEntryIndex(args.PrevLogIndex)
	raft.log3.rwMutex.RUnlock()
	if !found || logEntry.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = currentTerm
		return
	}

	if len(args.Entries) == 0 {
		reply.Success = true
		reply.Term = currentTerm
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	slices.SortFunc(args.Entries, func(a LogEntry, b LogEntry) int {
		return int(b.Index - a.Index) //sort in decreasing order
	})
	raft.log3.rwMutex.Lock()
	for _, remoteLogEntry := range args.Entries {
		loc, ok := raft.log3.value.FindLocationByEntryIndex(remoteLogEntry.Index)
		if ok {
			if raft.log3.value.at(loc).Term != remoteLogEntry.Term {
				raft.log3.value.setLogEntrySlice(raft.log3.value.subSlice(0, loc))
			}
		}
	}

	// Append any new entries not already in the log
	slices.SortFunc(args.Entries, func(a LogEntry, b LogEntry) int {
		return int(a.Index - b.Index) //sort in increasing order
	})
	for _, remoteLogEntry := range args.Entries {
		_, found := raft.log3.value.FindLocationByEntryIndex(remoteLogEntry.Index)
		if !found {
			raft.log3.value.append(remoteLogEntry)
		}
	}
	raft.log3.rwMutex.Unlock()

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	raft.commitIndex6.rwMutex.Lock()
	if args.LeaderCommitIdx > raft.commitIndex6.value {
		//The min in the final step (#5) of AppendEntries is necessary,
		//and it needs to be computed with the index of the last new entry.
		//It is not sufficient to simply have the function that applies things
		//from your log between lastApplied and commitIndex stop when it reaches the end of your log.
		//This is because you may have entries in your log that differ from the leader’s log after the entries that
		//the leader sent you (which all match the ones in your log).
		//Because #3 dictates that you only truncate your log if you have conflicting entries, those won’t be removed,
		//and if leaderCommit is beyond the entries the leader sent you, you may apply incorrect entries.
		raft.commitIndex6.value = min(args.LeaderCommitIdx, args.Entries[len(args.Entries)-1].Index)
		raft.applyCommittedCommand()
	}
	reply.Success = true
	reply.Term = currentTerm
	raft.commitIndex6.rwMutex.Unlock()
	return
}

// example ProcessRequestVoteRequest RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64 // candidate's value
	CandidateID  int   // candidate requesting vote
	LastLogIndex int64 // index of candidate’s last log entry (§5.4)
	LastLogTerm  int64 // value of candidate’s last log entry (§5.4)
}

// example ProcessRequestVoteRequest RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64 //current value, for candidate to update itself
	VoteGranted bool  // true means candidate received vote
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
	ok := raft.peers[server].Call("Raft.ProcessRequestVoteRequest", args, reply)
	return ok
}

// example ProcessRequestVoteRequest RPC handler.
func (raft *Raft) ProcessRequestVoteRequest(args *RequestVoteArgs, reply *RequestVoteReply) {
	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	raft.convertToFollowerGivenLargerTerm(args.Term)

	// Your code here (2A, 2B).
	// hold the read lock of currentTerm, so it is consistent and cannot change during the process
	raft.currentTerm1.rwMutex.RLock()
	defer raft.currentTerm1.rwMutex.RUnlock()
	currentTerm := raft.currentTerm1.value
	if args.Term < currentTerm {
		reply.VoteGranted = false
		reply.Term = currentTerm
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	raft.votedFor2.rwMutex.Lock()
	raft.log3.rwMutex.RLock()
	if (raft.votedFor2.value == VOTED_FOR_NO_ONE || raft.votedFor2.value == args.CandidateID) &&
		(args.LastLogTerm > raft.log3.value.Last().Term ||
			(args.LastLogTerm == raft.log3.value.Last().Term && args.LastLogIndex >= raft.log3.value.Last().Index)) {
		reply.VoteGranted = true
		raft.votedFor2.value = args.CandidateID
		reply.Term = currentTerm
		// restart your election timer if you grant a vote to another peer.
		raft.electionTimer10.Reset(getElectionTimeout())
	} else {
		reply.VoteGranted = false
		reply.Term = currentTerm
	}
	raft.log3.rwMutex.RUnlock()
	raft.votedFor2.rwMutex.Unlock()
	return
}

// if you have already voted in the current term, and an incoming ProcessRequestVoteRequest RPC has a higher term that you,
// you should first step down and adopt their term (thereby resetting votedFor),
// and then handle the RPC, which will result in you granting the vote!
func (raft *Raft) convertToFollowerGivenLargerTerm(term int64) bool {
	raft.currentTerm1.rwMutex.Lock()
	defer raft.currentTerm1.rwMutex.Unlock()
	raft.votedFor2.rwMutex.Lock()
	defer raft.votedFor2.rwMutex.Unlock()
	raft.role4.rwMutex.Lock()
	defer raft.role4.rwMutex.Unlock()
	if term > raft.currentTerm1.value {
		raft.role4.value = FOLLOWER
		raft.currentTerm1.value = term
		//reset the votedFor in the new term
		raft.votedFor2.value = VOTED_FOR_NO_ONE
		select {
		case <-raft.roleChannel5:
			break
		default:
			break
		}
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
	// Your code here (2B).
	raft.role4.rwMutex.RLock()
	role := raft.role4.value
	raft.role4.rwMutex.RUnlock()
	if role != LEADER {
		index := -1
		term := -1
		return index, term, false
	}

	// TODO: what if the server gets killed or becomes another role during the following process
	// this function should return gracefully...??
	raft.currentTerm1.rwMutex.RLock()
	defer raft.currentTerm1.rwMutex.RUnlock()
	raft.log3.rwMutex.RLock()
	defer raft.log3.rwMutex.RUnlock()
	potentialCommittedIndex := raft.log3.value.Last().Index + 1
	go raft.propose(command)
	return int(potentialCommittedIndex), int(raft.currentTerm1.value), true
}

// If command received from client: append entry to local log,
// todo: respond after entry applied to state machine (§5.3)
func (raft *Raft) propose(command interface{}) {
	raft.currentTerm1.rwMutex.RLock()
	raft.log3.rwMutex.Lock()
	// append the new entry to the local log
	raft.log3.value.append(LogEntry{
		Term:    raft.currentTerm1.value,
		Index:   raft.log3.value.Last().Index + 1,
		Command: command,
	})

	// TODO: the following tasks could be proposal-driven or be put into a single coroutine
	// If last log index ≥ nextIndex for a follower:
	// send AppendEntries RPC with log entries starting at nextIndex
	var wg sync.WaitGroup
	for peerIdx, _ := range raft.peers {
		raft.nextIndexSlice8[peerIdx].rwMutex.RLock()
		if peerIdx != raft.me && raft.log3.value.Last().Index >= raft.nextIndexSlice8[peerIdx].value {
			wg.Add(1)
			go func(peerIdx int) {
				defer wg.Done()
				raft.trySendingAppendEntriesTo(peerIdx, command)
			}(peerIdx)
		}
		raft.nextIndexSlice8[peerIdx].rwMutex.RUnlock()
	}
	raft.log3.rwMutex.Unlock()
	raft.currentTerm1.rwMutex.RUnlock()
	wg.Wait()

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	newCommitIndex := raft.commitIndex6.Load() + 1
	for true {
		largerMatchCount := 0
		for idx, _ := range raft.matchIndexSlice9 {
			if raft.matchIndexSlice9[idx].Load() >= newCommitIndex {
				largerMatchCount += 1
			}
		}

		if largerMatchCount > len(raft.peers)/2 {
			logEntry, found := raft.log3.FindEntryByEntryIndex(newCommitIndex)
			if found && logEntry.Term == raft.currentTerm1.get() {
				// commit
				raft.commitIndex6.Store(newCommitIndex)
				raft.applyCommittedCommand()
			} else {
				break // todo: break here? or use a new coroutine to periodically execute the block of code
			}
		} else {
			break
		}
	}
}

func (raft *Raft) trySendingAppendEntriesTo(peerIdx int, command interface{}) {
	if raft.role4.get() == LEADER {
		logEntry := LogEntry{
			Term:    raft.currentTerm1.get(),
			Index:   raft.nextIndexSlice8[peerIdx].Load(),
			Command: command,
		}
		prevLogEntry, found := raft.log3.FindEntryByEntryIndex(logEntry.Index - 1)
		if found == false {
			panic("not found")
		}
		appendEntriesArgs := AppendEntriesArgs{
			Term:            raft.currentTerm1.get(),
			LeaderId:        raft.me,
			PrevLogIndex:    prevLogEntry.Index,
			PrevLogTerm:     prevLogEntry.Term,
			Entries:         []LogEntry{logEntry},
			LeaderCommitIdx: raft.commitIndex6.Load(),
		}
		appendEntriesReply := AppendEntriesReply{}
		ok := raft.SendAppendEntriesRequest(peerIdx, &appendEntriesArgs, &appendEntriesReply)
		if ok {
			isLarger := raft.convertToFollowerGivenLargerTerm(appendEntriesReply.Term)
			if !isLarger {
				if appendEntriesArgs.Term == raft.currentTerm1.get() {
					if appendEntriesReply.Success {
						//If successful: update nextIndex and matchIndex for follower (§5.3)
						//A related, but not identical problem is that of assuming that your state has not changed between when you sent the RPC, and when you received the reply. A good example of this is setting matchIndex = nextIndex - 1, or matchIndex = len(log) when you receive a response to an RPC. This is not safe, because both of those values could have been updated since when you sent the RPC. Instead, the correct thing to do is update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally.
						matchIndex := appendEntriesArgs.PrevLogIndex + int64(len(appendEntriesArgs.Entries))
						raft.nextIndexSlice8[peerIdx].Store(matchIndex + 1)
						raft.matchIndexSlice9[peerIdx].Store(matchIndex)
					} else {
						//If AppendEntries fails because of log inconsistency:
						//decrement nextIndex and retry (§5.3)
						raft.nextIndexSlice8[peerIdx].Add(-1)
						raft.trySendingAppendEntriesTo(peerIdx, command)
					}
				}
			}
		}
		return
	} else {
		return
	}
}

func (raft *Raft) applyCommittedCommand() {
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	// TODO: is applyChannel guaranteed to be safe?
	if raft.commitIndex6.Load() > raft.lastApplied7.Load() {
		raft.lastApplied7.Add(1)
		raft.applyChannel11 <- ApplyMsg{
			CommandValid:  true,
			Command:       raft.log3.at(int(raft.lastApplied7.Load())).Command,
			CommandIndex:  int(raft.lastApplied7.Load()),
			SnapshotValid: false, //todo: update those none values
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
	}
	return
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
	// Your code here, if desired.
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
		<-raft.electionTimer10.C
		if raft.role4.get() != LEADER { // TODO: verify if it's required to check the role
			if raft.votedFor2.get() == VOTED_FOR_NO_ONE { // TODO: granting vote?
				raft.startElection()
			}
		}
	}
}

// Candidates (§5.2):
func (raft *Raft) startElection() {
	// On conversion to candidate, start election:
	raft.role4.set(CANDIDATE)
	// • Increment currentTerm
	raft.currentTerm1.set(raft.currentTerm1.get() + 1)
	// • Vote for self
	voteChannel := make(chan bool, len(raft.peers))
	voteChannel <- true
	raft.votedFor2.set(raft.me)
	// • Reset election timer
	raft.electionTimer10.Reset(getElectionTimeout())
	// • Send ProcessRequestVoteRequest RPCs to all other servers
	for peerIdx := range raft.peers {
		if peerIdx != raft.me {
			go func(peerIdx int) {
				requestVoteArgs := RequestVoteArgs{
					Term:         raft.currentTerm1.get(),
					CandidateID:  raft.me,
					LastLogIndex: raft.log3.Last().Index,
					LastLogTerm:  raft.log3.Last().Term,
				}
				requestVoteReply := RequestVoteReply{}
				ok := raft.sendRequestVote(peerIdx, &requestVoteArgs, &requestVoteReply)
				if ok {
					isLarger := raft.convertToFollowerGivenLargerTerm(requestVoteReply.Term)
					if !isLarger {
						if requestVoteArgs.Term == raft.currentTerm1.get() {
							if requestVoteReply.VoteGranted {
								voteChannel <- true
							}
						}
					}
				}
			}(peerIdx)
		}
	}

	// If votes received from the majority of servers: become leader
	// todo: If AppendEntries RPC received from new leader: convert to follower
	// If election timeout elapses: start new election
	voteSum := 0
	for raft.killed() == false && raft.role4.get() == CANDIDATE {
		select {
		case <-voteChannel:
			voteSum += 1
			if voteSum > len(raft.peers)/2 {
				raft.convertToLeader()
				return
			}
		case raft.roleChannel5 <- raft.role4.get():
			raft.electionTimer10.Reset(getElectionTimeout())
			return
		case <-raft.electionTimer10.C:
			raft.startElection()
		}
	}
	return
}

func (raft *Raft) convertToLeader() {
	raft.log3.rwMutex.RLock()
	defer raft.log3.rwMutex.RUnlock()
	raft.role4.rwMutex.Lock()
	defer raft.role4.rwMutex.Unlock()
	raft.role4.value = LEADER

	//reinitialize
	for idx, _ := range raft.nextIndexSlice8 {
		raft.nextIndexSlice8[idx].rwMutex.Lock()
		raft.nextIndexSlice8[idx].value = raft.log3.value.Last().Index + 1
		raft.nextIndexSlice8[idx].rwMutex.Unlock()
	}

	for idx, _ := range raft.matchIndexSlice9 {
		raft.matchIndexSlice9[idx].rwMutex.Lock()
		raft.matchIndexSlice9[idx].value = 0
		raft.matchIndexSlice9[idx].rwMutex.Unlock()
	}

	//Upon election: send initial empty AppendEntries RPCs
	//(heartbeat) to each server; repeat during idle periods to
	//prevent election timeouts (§5.2)
	go func() {
		heartbeatTicker := time.NewTicker(HEARTBEAT_TIMEOUT)
		for raft.killed() == false {
			raft.role4.rwMutex.RLock()
			if raft.role4.value == LEADER {
				select {
				case <-heartbeatTicker.C:
					raft.sendHeartbeat()
				}
			}
			raft.role4.rwMutex.RUnlock()
		}
	}()
}

const HEARTBEAT_TIMEOUT = time.Millisecond * 100

func (raft *Raft) sendHeartbeat() {
	raft.currentTerm1.rwMutex.RLock()
	raft.log3.rwMutex.RLock()
	raft.commitIndex6.rwMutex.RLock()
	appendEntriesArgs := AppendEntriesArgs{
		Term:            raft.currentTerm1.value,
		LeaderId:        raft.me,
		PrevLogIndex:    raft.log3.value.Last().Index, // TODO: what's the value of the prev? Should the heartbeat also performs as normal AppendEntries
		PrevLogTerm:     raft.log3.value.Last().Term,
		Entries:         []LogEntry{},
		LeaderCommitIdx: raft.commitIndex6.value,
	}
	raft.commitIndex6.rwMutex.RUnlock()
	raft.log3.rwMutex.RUnlock()
	raft.currentTerm1.rwMutex.RUnlock()
	for peerIdx, _ := range raft.peers {
		if peerIdx != raft.me {
			go func(peerIdx int, appendEntriesArgs AppendEntriesArgs) {
				appendEntriesReply := AppendEntriesReply{}
				ok := raft.SendAppendEntriesRequest(peerIdx, &appendEntriesArgs, &appendEntriesReply)
				if ok {
					//todo: appendEntriesReply.Success
					raft.convertToFollowerGivenLargerTerm(appendEntriesReply.Term)
				}
			}(peerIdx, appendEntriesArgs)
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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      LIVE,
		currentTerm1: ValueWithRWMutex[int64]{
			value:   0,
			rwMutex: sync.RWMutex{},
		},
		votedFor2: ValueWithRWMutex[int]{
			value:   VOTED_FOR_NO_ONE,
			rwMutex: sync.RWMutex{},
		},
		log3: ValueWithRWMutex[Log]{
			value: Log{
				logEntrySlice: []LogEntry{
					{
						Term:    0,
						Index:   0,
						Command: nil,
					},
				},
			},
			rwMutex: sync.RWMutex{},
		},
		role4: ValueWithRWMutex[int]{
			value:   FOLLOWER,
			rwMutex: sync.RWMutex{},
		},
		roleChannel5: make(chan int),
		commitIndex6: ValueWithRWMutex[int64]{
			value:   0,
			rwMutex: sync.RWMutex{},
		},
		lastApplied7: ValueWithRWMutex[int64]{
			value:   0,
			rwMutex: sync.RWMutex{},
		},
		nextIndexSlice8:  make([]ValueWithRWMutex[int64], len(peers)),
		matchIndexSlice9: make([]ValueWithRWMutex[int64], len(peers)),
		electionTimer10:  time.NewTimer(getElectionTimeout()),
		applyChannel11:   applyCh,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
