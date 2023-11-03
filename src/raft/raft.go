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

type ValueWithRWMutex[T any] struct {
	value   T
	rwMutex sync.RWMutex
}

func (valueWithRWMutex *ValueWithRWMutex[T]) get() T {
	valueWithRWMutex.rwMutex.RLock()
	defer valueWithRWMutex.rwMutex.RUnlock()
	return valueWithRWMutex.value
}

func (valueWithRWMutex *ValueWithRWMutex[T]) set(value T) {
	valueWithRWMutex.rwMutex.Lock()
	defer valueWithRWMutex.rwMutex.Unlock()
	valueWithRWMutex.value = value
}

//TODO: However, Figure 2 generally doesn’t discuss what you should do when you
//get old RPC replies. From experience, we have found that by far the simplest
//thing to do is to first record the term in the reply (it may be higher than
//your current term), and then to compare the current term with the term you
//sent in your original RPC. If the two are different, drop the reply and
//return. Only if the two terms are the same should you continue processing the
//reply. There may be further optimizations you can do here with some clever
//protocol reasoning, but this approach seems to work well. And not doing it
//leads down a long, winding path of blood, sweat, tears and despair.

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
	currentTerm ValueWithRWMutex[int64] // latest value sever has seen
	votedFor    ValueWithRWMutex[int]   // candidateId that received vote in current value
	log         LogWithRWMutex          // log entries

	// volatile state on all servers
	commitIndex atomic.Int64 // index of highest log entry known to be committed
	lastApplied atomic.Int64 // index of highest log entry applied to state machine

	// volatile state on leaders
	nextIndexSlice  []atomic.Int64 // for each server, index of the next log entry to send to that server
	matchIndexSlice []atomic.Int64 // for each server, index of highest log entry known to be replicated on server

	// follower
	//TODO: The management of the election timeout is a common source of
	//headaches. Perhaps the simplest plan is to maintain a variable in the
	//Raft struct containing the last time at which the peer heard from the
	//leader, and to have the election timeout goroutine periodically check
	//to see whether the time since then is greater than the timeout period.
	//It's easiest to use time.Sleep() with a small constant argument to
	//drive the periodic checks. Don't use time.Ticker and time.Timer;
	//they are tricky to use correctly.
	electionTimer *time.Timer

	// leader
	roleChannel  SingleValueChannel[int]
	applyChannel chan ApplyMsg
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

type SingleValueChannel[T any] struct {
	channel chan T // the channel size can only be one
}

func (singleValueChannel *SingleValueChannel[T]) forcePush(value T) {
	select {
	case <-singleValueChannel.channel:
		singleValueChannel.channel <- value
	default:
		singleValueChannel.channel <- value
	}
}

// every take must be followed by a forcePush
func (singleValueChannel *SingleValueChannel[T]) take() <-chan T {
	return singleValueChannel.channel
}

func (singleValueChannel *SingleValueChannel[T]) peek() T {
	value := <-singleValueChannel.take()
	singleValueChannel.forcePush(value)
	return value
}

func getElectionTimeout() time.Duration {
	return time.Millisecond * time.Duration(rand.Intn(150)+300)
}

type LogWithRWMutex struct {
	logEntrySlice []LogEntry
	rwMutex       sync.RWMutex
}

func (log *LogWithRWMutex) at(idx int) LogEntry {
	log.rwMutex.RLock()
	defer log.rwMutex.RUnlock()
	return log.logEntrySlice[idx]
}

func (log *LogWithRWMutex) Last() LogEntry {
	log.rwMutex.RLock()
	defer log.rwMutex.RUnlock()
	return log.logEntrySlice[len(log.logEntrySlice)-1]
}

func (log *LogWithRWMutex) FindLocationByEntryIndex(entryIndex int64) (int, bool) {
	log.rwMutex.RLock()
	defer log.rwMutex.RUnlock()
	return sort.Find(len(log.logEntrySlice), func(i int) int {
		return int(entryIndex - log.logEntrySlice[i].Index)
	})
}

func (log *LogWithRWMutex) FindEntryByEntryIndex(entryIndex int64) (LogEntry, bool) {
	log.rwMutex.RLock()
	defer log.rwMutex.RUnlock()
	loc, found := log.FindLocationByEntryIndex(entryIndex)
	if !found {
		return LogEntry{}, false
	} else {
		return log.logEntrySlice[loc], true
	}
}

func (log *LogWithRWMutex) subSlice(start int, end int) []LogEntry {
	log.rwMutex.RLock()
	defer log.rwMutex.RUnlock()
	return log.logEntrySlice[start:end]
}

func (log *LogWithRWMutex) setLogEntrySlice(logEntrySlice []LogEntry) {
	log.rwMutex.Lock()
	defer log.rwMutex.Unlock()
	log.logEntrySlice = logEntrySlice
}

func (log *LogWithRWMutex) append(logEntry LogEntry) {
	log.rwMutex.Lock()
	defer log.rwMutex.Unlock()
	log.logEntrySlice = append(log.logEntrySlice, logEntry)
}

type LogEntry struct {
	Term    int64       // value when entry was received by leader
	Index   int64       // index of the log entry
	Command interface{} // command for state machine
}

// return currentTerm and whether this server
// believes it is the leader.
func (raft *Raft) GetState() (int, bool) {
	return int(raft.currentTerm.get()), raft.roleChannel.peek() == LEADER
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
	// Reply false if value < currentTerm (§5.1)
	if args.Term < raft.currentTerm.get() {
		reply.Success = false
		reply.Term = raft.currentTerm.get()
		return
	}

	// set timeout flag
	// you get an AppendEntries RPC from the current leader
	// (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer)
	raft.electionTimer.Reset(getElectionTimeout())

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > raft.currentTerm.get() {
		raft.convertToFollower(args.Term)
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose value matches prevLogTerm (§5.3)
	logEntry, found := raft.log.FindEntryByEntryIndex(args.PrevLogIndex)
	if !found || logEntry.Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = raft.currentTerm.get()
		return
	}

	if len(args.Entries) == 0 { // TODO: should an empty entry avoid executing the following program?
		reply.Success = true
		reply.Term = raft.currentTerm.get()
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	slices.SortFunc(args.Entries, func(a LogEntry, b LogEntry) int {
		return int(b.Index - a.Index) //sort in decreasing order
	})
	for _, remoteLogEntry := range args.Entries {
		loc, ok := raft.log.FindLocationByEntryIndex(remoteLogEntry.Index)
		if ok {
			if raft.log.at(loc).Term != remoteLogEntry.Term {
				raft.log.setLogEntrySlice(raft.log.subSlice(0, loc))
			}
		}
	}

	// Append any new entries not already in the log
	slices.SortFunc(args.Entries, func(a LogEntry, b LogEntry) int {
		return int(a.Index - b.Index) //sort in increasing order
	})
	for _, remoteLogEntry := range args.Entries {
		_, found := raft.log.FindLocationByEntryIndex(remoteLogEntry.Index)
		if !found {
			raft.log.append(remoteLogEntry)
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommitIdx > raft.commitIndex.Load() {
		raft.commitIndex.Store(min(args.LeaderCommitIdx, args.Entries[len(args.Entries)-1].Index))
		raft.applyCommittedCommand()
	}
	reply.Success = true
	reply.Term = raft.currentTerm.get()
	return
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64 // candidate's value
	CandidateID  int   // candidate requesting vote
	LastLogIndex int64 // index of candidate’s last log entry (§5.4)
	LastLogTerm  int64 // value of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64 //current value, for candidate to update itself
	VoteGranted bool  // true means candidate received vote
}

// example RequestVote RPC handler.
func (raft *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term < raft.currentTerm.get() {
		reply.VoteGranted = false
		reply.Term = raft.currentTerm.get()
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > raft.currentTerm.get() {
		raft.convertToFollower(args.Term)
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if (raft.votedFor.get() == VOTED_FOR_NO_ONE || raft.votedFor.get() == args.CandidateID) &&
		(args.LastLogTerm > raft.log.Last().Term ||
			(args.LastLogTerm == raft.log.Last().Term && args.LastLogIndex >= raft.log.Last().Index)) {
		reply.VoteGranted = true
		reply.Term = raft.currentTerm.get()
		// restart your election timer if you grant a vote to another peer.
		raft.electionTimer.Reset(getElectionTimeout())
		return
	} else {
		reply.VoteGranted = false
		reply.Term = raft.currentTerm.get()
		return
	}
}

func (raft *Raft) convertToFollower(term int64) {
	raft.currentTerm.set(term)
	raft.roleChannel.forcePush(FOLLOWER)
	//if you have already voted in the current term, and an incoming RequestVote RPC has a higher term that you, you should first step down and adopt their term (thereby resetting votedFor), and then handle the RPC, which will result in you granting the vote!
	//reset the votedFor in the new term
	raft.votedFor.set(VOTED_FOR_NO_ONE)
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
func (raft *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := raft.peers[server].Call("Raft.RequestVote", args, reply)
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
// if it's ever committed.
// the second return value is the current term.
// the third return value is true if this server believes it is the leader.
func (raft *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	if raft.roleChannel.peek() != LEADER {
		index := -1
		term := -1
		return index, term, false
	}

	// TODO: what if the server gets killed during the following process
	potentialCommittedIndex := raft.log.Last().Index + 1
	go raft.propose(command)
	return int(potentialCommittedIndex), int(raft.currentTerm.get()), true
}

func (raft *Raft) propose(command interface{}) {
	// If command received from client: append entry to local log,
	// todo: respond after entry applied to state machine (§5.3)
	raft.log.append(LogEntry{
		Term:    raft.currentTerm.get(),
		Index:   raft.log.Last().Index + 1,
		Command: command,
	})

	var wg sync.WaitGroup
	for peerIdx, _ := range raft.peers {
		//If last log index ≥ nextIndex for a follower:
		//send AppendEntries RPC with log entries starting at nextIndex
		if peerIdx != raft.me && raft.log.Last().Index >= raft.nextIndexSlice[peerIdx].Load() {
			wg.Add(1)
			go func(peerIdx int) {
				defer wg.Done()
				raft.trySendingAppendEntriesTo(peerIdx, command)
			}(peerIdx)
		}
	}
	wg.Wait()

	// If there exists an N such that N > commitIndex, a majority
	// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
	// set commitIndex = N (§5.3, §5.4).
	newCommitIndex := raft.commitIndex.Load() + 1
	for true {
		largerMatchCount := 0
		for idx, _ := range raft.matchIndexSlice {
			if raft.matchIndexSlice[idx].Load() >= newCommitIndex {
				largerMatchCount += 1
			}
		}

		if largerMatchCount > len(raft.peers)/2 {
			logEntry, found := raft.log.FindEntryByEntryIndex(newCommitIndex)
			if found && logEntry.Term == raft.currentTerm.get() {
				// commit
				raft.commitIndex.Store(newCommitIndex)
				raft.applyCommittedCommand()
			} else {
				break // todo: break here? or use a new coroutine to periodically execute the block of code
			}
		} else {
			break
		}
	}
}

func (raft *Raft) applyCommittedCommand() {
	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine (§5.3)
	// TODO: is applyChannel guaranteed to be safe?
	if raft.commitIndex.Load() > raft.lastApplied.Load() {
		raft.lastApplied.Add(1)
		raft.applyChannel <- ApplyMsg{
			CommandValid:  true,
			Command:       raft.log.at(int(raft.lastApplied.Load())).Command,
			CommandIndex:  int(raft.lastApplied.Load()),
			SnapshotValid: false, //todo: update those none values
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
	}
	return
}

func (raft *Raft) trySendingAppendEntriesTo(peerIdx int, command interface{}) {
	if raft.roleChannel.peek() == LEADER {
		logEntry := LogEntry{
			Term:    raft.currentTerm.get(),
			Index:   raft.nextIndexSlice[peerIdx].Load(),
			Command: command,
		}
		prevLogEntry, found := raft.log.FindEntryByEntryIndex(logEntry.Index - 1)
		if found == false {
			panic("not found")
		}
		appendEntriesArgs := AppendEntriesArgs{
			Term:            raft.currentTerm.get(),
			LeaderId:        raft.me,
			PrevLogIndex:    prevLogEntry.Index,
			PrevLogTerm:     prevLogEntry.Term,
			Entries:         []LogEntry{logEntry},
			LeaderCommitIdx: raft.commitIndex.Load(),
		}
		appendEntriesReply := AppendEntriesReply{}
		ok := raft.SendAppendEntriesRequest(peerIdx, &appendEntriesArgs, &appendEntriesReply)
		if ok {
			if appendEntriesReply.Term > raft.currentTerm.get() {
				raft.convertToFollower(appendEntriesReply.Term)
				return
			} else {
				if appendEntriesArgs.Term != raft.currentTerm.get() {
					return
				} else {
					if appendEntriesReply.Success {
						//If successful: update nextIndex and matchIndex for follower (§5.3)
						//A related, but not identical problem is that of assuming that your state has not changed between when you sent the RPC, and when you received the reply. A good example of this is setting matchIndex = nextIndex - 1, or matchIndex = len(log) when you receive a response to an RPC. This is not safe, because both of those values could have been updated since when you sent the RPC. Instead, the correct thing to do is update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally.
						matchIndex := appendEntriesArgs.PrevLogIndex + int64(len(appendEntriesArgs.Entries))
						raft.nextIndexSlice[peerIdx].Store(matchIndex + 1)
						raft.matchIndexSlice[peerIdx].Store(matchIndex)
					} else {
						//If AppendEntries fails because of log inconsistency:
						//decrement nextIndex and retry (§5.3)
						raft.nextIndexSlice[peerIdx].Add(-1)
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
		<-raft.electionTimer.C
		if raft.roleChannel.peek() != LEADER { // TODO: verify if it's required to check the role
			if raft.votedFor.get() == VOTED_FOR_NO_ONE { // TODO: granting vote?
				raft.startElection()
			}
		}
	}
}

// Candidates (§5.2):
func (raft *Raft) startElection() {
	// On conversion to candidate, start election:
	raft.roleChannel.forcePush(CANDIDATE)
	// • Increment currentTerm
	raft.currentTerm.set(raft.currentTerm.get() + 1)
	// • Vote for self
	voteChannel := make(chan bool, len(raft.peers))
	voteChannel <- true
	raft.votedFor.set(raft.me)
	// • Reset election timer
	raft.electionTimer.Reset(getElectionTimeout())
	// • Send RequestVote RPCs to all other servers
	for peerIdx := range raft.peers {
		if peerIdx != raft.me {
			go func(peerIdx int) {
				requestVoteArgs := RequestVoteArgs{
					Term:         raft.currentTerm.get(),
					CandidateID:  raft.me,
					LastLogIndex: raft.log.Last().Index,
					LastLogTerm:  raft.log.Last().Term,
				}
				requestVoteReply := RequestVoteReply{}
				ok := raft.sendRequestVote(peerIdx, &requestVoteArgs, &requestVoteReply)
				if ok {
					if requestVoteReply.Term > raft.currentTerm.get() {
						raft.convertToFollower(requestVoteReply.Term)
						return
					} else {
						if requestVoteArgs.Term != raft.currentTerm.get() {
							return
						} else {
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
	// If AppendEntries RPC received from new leader: convert to follower
	// If election timeout elapses: start new election
	voteSum := 0
	for raft.killed() == false && raft.roleChannel.peek() == CANDIDATE {
		select {
		case <-voteChannel:
			voteSum += 1
			if voteSum > len(raft.peers)/2 {
				raft.convertToLeader()
				return
			}
		case role := <-raft.roleChannel.take(): // the role here can only be FOLLOWER because the server is now in election
			raft.roleChannel.forcePush(role)
			if role == FOLLOWER {
				raft.convertToFollower(raft.currentTerm.get())
				raft.electionTimer.Reset(getElectionTimeout())
				return
			}
		case <-raft.electionTimer.C:
			raft.startElection()
		}
	}
	return
}

func (raft *Raft) convertToLeader() {
	raft.roleChannel.forcePush(LEADER)
	for idx, _ := range raft.nextIndexSlice {
		raft.nextIndexSlice[idx].Store(raft.log.Last().Index + 1)
	}
	for idx, _ := range raft.matchIndexSlice { // TODO: does the matchIndex need to be reinitialized?
		raft.matchIndexSlice[idx].Store(0)
	}
	raft.Lead()
}

const HEARTBEAT_TIMEOUT = time.Millisecond * 100

func (raft *Raft) Lead() {
	//Upon election: send initial empty AppendEntries RPCs
	//(heartbeat) to each server; repeat during idle periods to
	//prevent election timeouts (§5.2)
	go func() {
		heartbeatTicker := time.NewTicker(HEARTBEAT_TIMEOUT)
		for raft.killed() == false && raft.roleChannel.peek() == LEADER {
			select {
			case <-heartbeatTicker.C:
				raft.sendHeartbeat()
			}
		}
	}()
}

func (raft *Raft) sendHeartbeat() {
	for peerIdx, _ := range raft.peers {
		if peerIdx != raft.me {
			go func(peerIdx int) {
				appendEntriesArgs := AppendEntriesArgs{
					Term:            raft.currentTerm.get(),
					LeaderId:        raft.me,
					PrevLogIndex:    raft.log.Last().Index, // TODO: what's the value of the prev? Should the heartbeat also performs as normal AppendEntries
					PrevLogTerm:     raft.log.Last().Term,
					Entries:         []LogEntry{},
					LeaderCommitIdx: raft.commitIndex.Load(),
				}
				appendEntriesReply := AppendEntriesReply{}
				ok := raft.SendAppendEntriesRequest(peerIdx, &appendEntriesArgs, &appendEntriesReply)
				if ok {
					//todo: appendEntriesReply.Success
					if appendEntriesReply.Term > raft.currentTerm.get() {
						raft.convertToFollower(appendEntriesReply.Term)
					} else {
						if appendEntriesArgs.Term != raft.currentTerm.get() {
							return
						}
					}
				}
			}(peerIdx)
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
	rf := &Raft{
		mu:        sync.Mutex{},
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      LIVE,
		currentTerm: ValueWithRWMutex[int64]{
			value:   0,
			rwMutex: sync.RWMutex{},
		},
		votedFor: ValueWithRWMutex[int]{value: VOTED_FOR_NO_ONE},
		log: LogWithRWMutex{
			logEntrySlice: []LogEntry{
				{
					Term:    0,
					Index:   0,
					Command: nil,
				},
			},
		},
		commitIndex:     atomic.Int64{},
		lastApplied:     atomic.Int64{},
		nextIndexSlice:  make([]atomic.Int64, len(peers)),
		matchIndexSlice: make([]atomic.Int64, len(peers)),
		electionTimer:   time.NewTimer(getElectionTimeout()),
		roleChannel:     SingleValueChannel[int]{channel: make(chan int, 1)},
		applyChannel:    applyCh,
	}
	for idx, _ := range rf.nextIndexSlice {
		rf.nextIndexSlice[idx].Store(rf.log.Last().Index + 1)
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.roleChannel.forcePush(FOLLOWER)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
