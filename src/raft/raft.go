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

import "sync"
import "labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "encoding/gob"


const minElectionTimeout = 150
const maxElectionTimeout = 350

const timeout_heartbeat = 100

const Follower = 2
const Candidate = 1
const Leader = 0

func getRandomTimeout() time.Duration {
	randTimeout := minElectionTimeout + rand.Intn(maxElectionTimeout-minElectionTimeout)
	return time.Duration(randTimeout)
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	Role                  int
	channelChangeRole     chan bool
	CurrentTerm           int
	VotedFor              int
	channelElectionWinner chan bool
	Log                   []LogEntry  // seria usado para replicação de logs
	totalVotes            int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.Role == Leader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
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
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int  
	LeaderId int
	Entries  []int  // seria usado para replicar entry
}

type AppendEntriesReply struct {
	Term    int 
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm

	if args.Term > rf.CurrentTerm {
		rf.Role = Follower
		rf.CurrentTerm = args.Term
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.channelChangeRole <- true
	}

	// TODO: Não teria que atualizar quem pede voto caso ele esteja desatualizado?

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm { 
		reply.Success = false
	} else if args.Term > rf.CurrentTerm {
		rf.Role = Follower
		rf.CurrentTerm = args.Term
		rf.VotedFor = args.LeaderId
		rf.channelChangeRole <- true
	} else {
		rf.Role = Follower
		rf.VotedFor = args.LeaderId
		rf.channelChangeRole <- true
	}

	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) triggerHeartbeat() {
	for server_i := 0; server_i < len(rf.peers); server_i++ {
		if server_i == rf.me {  
			continue  // Pula heartbeat caso igual a ele mesmo
		}

		rf.mu.Lock()
		server := server_i
		ae_args := &AppendEntriesArgs{}
		ae_args.LeaderId = rf.me
		ae_args.Term = rf.CurrentTerm
		rf.mu.Unlock()

		go func() {
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(server, ae_args, &reply)
			rf.mu.Lock()

			if reply.Term > rf.CurrentTerm {
				rf.Role = Follower
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1
				rf.channelChangeRole <- true
			}
			
			rf.mu.Unlock()

		}()
	}
}

func (rf *Raft) initElectionProcess() {
	rv_args := RequestVoteArgs{}

	rf.mu.Lock()
	rv_args.CandidateId = rf.me
	rf.VotedFor = rf.me
	rv_args.Term = rf.CurrentTerm
	rf.totalVotes = 0
	rf.mu.Unlock()

	necessaryMajority := (len(rf.peers) / 2) + 1

	for server_i := 0; server_i < len(rf.peers); server_i++ {
		if server_i == rf.me {
			rf.mu.Lock()
			rf.totalVotes++ 
			rf.mu.Unlock()
			continue  // Pula processo caso igual a ele mesmo
		}
		go func(serverId int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(serverId, &rv_args, &reply) {
				rf.mu.Lock()
				if rf.Role == Candidate && reply.VoteGranted {
					rf.totalVotes++
					if rf.totalVotes >= necessaryMajority {
						rf.Role = Leader
						rf.channelElectionWinner <- true
					}
				}

				if reply.Term > rf.CurrentTerm {
					rf.Role = Follower
					rf.CurrentTerm = rv_args.Term
					rf.VotedFor = -1
					rf.channelChangeRole <- true
				}
				rf.mu.Unlock()
			}
		}(server_i)
	}
}

func (rf *Raft) raftCycle() {
	var timeout time.Duration

	for true {
		if rf.Role == Leader {
			leaderCycle:
				for true {
					rf.triggerHeartbeat()
					select {
						case <-time.After(timeout_heartbeat * time.Millisecond):
						case <-rf.channelChangeRole:
							break leaderCycle
					}
				}

		} else if rf.Role == Candidate {
			candidateCycle:
				for true {
					rf.initElectionProcess()
					timeout = getRandomTimeout()
					select {
						case <-time.After(timeout * time.Millisecond):
							rf.mu.Lock()
							rf.CurrentTerm++
							rf.mu.Unlock()
						case <-rf.channelElectionWinner:
							break candidateCycle
						case <-rf.channelChangeRole:
							break candidateCycle
					}
				}

		} else if rf.Role == Follower {
			followerCycle:
				for true {
					timeout = getRandomTimeout()
					select {
						case <-time.After(timeout * time.Millisecond):
							rf.mu.Lock()
							rf.CurrentTerm++
							rf.Role = Candidate
							rf.mu.Unlock()
							break followerCycle
						case <-rf.channelChangeRole:
						
					}
				}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.Role = Follower
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.totalVotes = 0
	rf.channelChangeRole = make(chan bool)
	rf.channelElectionWinner = make(chan bool)

	go rf.raftCycle()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
