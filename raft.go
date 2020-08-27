package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"mit/labgob"
	"sync"
	"time"
)
import "sync/atomic"
import "mit/labrpc"

type PeerState int

const (
	Follower PeerState = iota
	Candidate
	Leader
	Dead
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term         int
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type PersistState struct {
	CurrentTerm int
	Log         []LogEntry
	VotedFor    int
	NextIndex   map[int]int
	MatchIndex  map[int]int
	LastApplied int
	CommitIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Figure 2 for a description of what
	lastApplied        int
	commitIndex        int
	state              PeerState
	electionResetEvent time.Time

	// For leaders
	nextIndex  map[int]int
	matchIndex map[int]int

	applyCh        chan<- ApplyMsg
	applyReadyChan chan struct{}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	if err := e.Encode(PersistState{
		CurrentTerm: rf.currentTerm,
		Log:         rf.log,
		VotedFor:    rf.votedFor,
		NextIndex:   rf.nextIndex,
		MatchIndex:  rf.matchIndex,
		LastApplied: rf.lastApplied,
		CommitIndex: rf.commitIndex,
	}); err != nil {
		panic("Encode issue")
	}

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var persistState PersistState

	if d.Decode(&persistState) != nil {
		panic("Can not restore state")
	} else {
		rf.currentTerm = persistState.CurrentTerm
		rf.log = persistState.Log
		rf.votedFor = persistState.VotedFor
		rf.nextIndex = persistState.NextIndex
		rf.matchIndex = persistState.MatchIndex
		rf.lastApplied = persistState.LastApplied
		rf.commitIndex = persistState.CommitIndex
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Dead {
		return
	}

	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	if rf.currentTerm == args.Term && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.electionResetEvent = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Dead {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rf.currentTerm {
		if rf.state != Follower {
			rf.becomeFollower(args.Term)
		}
		rf.electionResetEvent = time.Now()

		if args.PrevLogIndex == -1 || (args.PrevLogIndex < len(rf.log) && args.PrevLogTerm == rf.log[args.PrevLogIndex].Term) {
			reply.Success = true
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0

			for {
				if logInsertIndex >= len(rf.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if rf.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}

			if newEntriesIndex < len(args.Entries) {
				rf.log = append(rf.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
			}

			// Set commit index.
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = len(rf.log) - 1
				if args.LeaderCommit < len(rf.log)-1 {
					rf.commitIndex = args.LeaderCommit
				}

				rf.applyReadyChan <- struct{}{}
			}
		}
	}

	reply.Term = rf.currentTerm
	rf.persist()
}

func (rf *Raft) debug(msg string) {
	fmt.Printf("[DEBUG: %v, Instance=%d] %s\n", time.Now(), rf.me, msg)
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.electionResetEvent = time.Now()

	go rf.runElectionTimer()
}

func (rf *Raft) runElectionTimer() {
	timeoutDuration := time.Duration(550+rand.Intn(300)) * time.Millisecond
	rf.mu.Lock()
	termStarted := rf.currentTerm
	rf.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		rf.mu.Lock()
		if rf.state != Candidate && rf.state != Follower {
			rf.mu.Unlock()
			return
		}

		if termStarted != rf.currentTerm {
			rf.mu.Unlock()
			return
		}

		if elapsed := time.Since(rf.electionResetEvent); elapsed >= timeoutDuration {
			rf.startElection()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() {
	rf.state = Candidate
	rf.currentTerm += 1
	savedCurrentTerm := rf.currentTerm
	rf.electionResetEvent = time.Now()
	rf.votedFor = rf.me

	var votesReceived int32 = 1

	for _, peer := range rf.peers {
		go func(peer *labrpc.ClientEnd) {
			rf.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := rf.lastLogIndexAndTerm()
			rf.mu.Unlock()

			args := &RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  rf.me,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}

			var reply RequestVoteReply

			if ok := peer.Call("Raft.RequestVote", args, &reply); ok == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.state != Candidate {
					return
				}

				if reply.Term > savedCurrentTerm {
					rf.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votes := int(atomic.AddInt32(&votesReceived, 1))
						if votes*2 > len(rf.peers)+1 {
							rf.startLeader()
							return
						}
					}
				}
			}
		}(peer)
	}

	go rf.runElectionTimer()
}

func (rf *Raft) startLeader() {
	rf.state = Leader

	go func() {
		ticker := time.NewTicker(150 * time.Millisecond)
		defer ticker.Stop()

		for {
			rf.leaderSendHeartbeats()
			<-ticker.C

			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		}
	}()
}

func (rf *Raft) leaderSendHeartbeats() {
	rf.mu.Lock()
	savedCurrentTerm := rf.currentTerm
	rf.mu.Unlock()

	for id, peer := range rf.peers {
		// do not send itself
		if rf.me == id {
			continue
		}

		go func(peer *labrpc.ClientEnd, id int) {
			rf.mu.Lock()
			ni := rf.nextIndex[id]
			prevLogIndex := ni - 1
			prevLogTerm := -1
			if prevLogIndex >= 0 {
				prevLogTerm = rf.log[prevLogIndex].Term
			}
			entries := rf.log[ni:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			rf.mu.Unlock()

			var reply AppendEntriesReply
			if ok := peer.Call("Raft.AppendEntries", args, &reply); ok == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > savedCurrentTerm {
					rf.becomeFollower(reply.Term)
					return
				}

				if rf.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						rf.nextIndex[id] = ni + len(entries)
						rf.matchIndex[id] = rf.nextIndex[id] - 1

						savedCommitIndex := rf.commitIndex
						for i := rf.commitIndex + 1; i < len(rf.log); i++ {
							if rf.log[i].Term == rf.currentTerm {
								matchCount := 1

								for innerId := range rf.peers {
									if rf.matchIndex[innerId] >= i {
										matchCount++
									}
								}

								// check majority
								if matchCount*2 >= len(rf.peers)+1 {
									rf.commitIndex = i
								}
							}
						}
						if rf.commitIndex != savedCommitIndex {
							rf.applyReadyChan <- struct{}{}
						}
					} else {
						rf.nextIndex[id] = ni - 1
					}
				}
				rf.persist()
			}
		}(peer, id)
	}
}

func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.log) > 0 {
		lastIndex := len(rf.log) - 1
		return lastIndex, rf.log[lastIndex].Term
	} else {
		return -1, -1
	}
}

func (rf *Raft) applyChanSender() {
	for range rf.applyReadyChan {
		rf.mu.Lock()
		savedTerm := rf.currentTerm
		savedLastApplied := rf.lastApplied
		var entries []LogEntry

		if rf.commitIndex > rf.lastApplied {
			entries = rf.log[rf.lastApplied+1 : rf.commitIndex+1]
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()

		for i, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: savedLastApplied + i + 1,
				Term:         savedTerm,
			}
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: rf.currentTerm})

		index, term := rf.lastLogIndexAndTerm()
		return index, term, true
	}

	return -1, -1, false
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyReadyChan = make(chan struct{}, 16)

	rf.state = Follower
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)

	rf.readPersist(persister.ReadRaftState())

	go rf.runElectionTimer()
	go rf.applyChanSender()

	return rf
}
