// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	electionRandomTimeout int
	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
// 这里接收到了config发来的参数
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	ID := c.ID
	etT := c.ElectionTick
	hbT := c.HeartbeatTick
	// sg := c.Storage
	// Msg := make([]pb.Message,size)
	vote := make(map[uint64]bool)
	Prs := make(map[uint64]*Progress)
	for _,i := range c.peers{
		vote[i] = false
		Prs[i] = new(Progress)
	}

	return &Raft {
		id : ID,
		Vote: None,
		heartbeatTimeout: hbT,
		electionRandomTimeout: etT, 
		electionTimeout: etT,
		electionElapsed: 0,
		heartbeatElapsed: 0,
		State: StateFollower,
		votes: vote,
		Term: None,
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		Term: r.Term,
		To: to,
		From: r.id,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	nanotime := int64(time.Now().UnixNano())
	rand.Seed(nanotime)
	switch r.State{
	case StateFollower: {
		r.electionElapsed = r.electionElapsed + 1
		if r.electionElapsed >= r.electionRandomTimeout {
			r.electionElapsed = 0
			r.electionRandomTimeout = rand.Intn(r.electionTimeout)+r.electionTimeout
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
	case StateCandidate: {
		r.electionElapsed = r.electionElapsed + 1
		if r.electionElapsed >= r.electionRandomTimeout {
			r.electionElapsed = 0
			r.electionRandomTimeout = rand.Intn(r.electionTimeout)+r.electionTimeout
			r.becomeCandidate()
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
			})
		}
	}
	case StateLeader: {
		r.heartbeatElapsed = r.heartbeatElapsed + 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
		}
	}
}

}


// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Term = r.Term + 1
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

switch r.State {
	case StateFollower:{
		switch m.MsgType{
		case pb.MessageType_MsgAppend:{
			if m.Term > r.Term{
				r.Term = m.Term
			}
			r.handleAppendEntries(m)
		}



			case pb.MessageType_MsgHup:{  //竞选超时产生候选人
				r.becomeCandidate()
				r.Step(pb.Message{                                 //身份转换后需要重新进入step中寻找对应的身份
					MsgType: pb.MessageType_MsgHup,
				})
			}

			case pb.MessageType_MsgHeartbeat:{   //接收到心跳
				if m.Term >= r.Term{
					r.becomeFollower(m.Term,m.From)
					r.Vote = m.From
				}
				r.msgs = append(r.msgs, pb.Message{
					MsgType: pb.MessageType_MsgHeartbeatResponse,
					To: m.From,
					From: r.id,
					Term: r.Term,
				})
			}

			case pb.MessageType_MsgRequestVote:{  //回应并且投票
				if m.Term > r.Term {       //如果任期大的来我们更新任期为较大的那个
					r.Term = m.Term
					r.Vote = None
				}
				R := true
				if r.Vote == None || r.Vote == m.From{
					r.Vote = m.From
					R = false
				}
					r.msgs = append(r.msgs, pb.Message{
						MsgType: pb.MessageType_MsgRequestVoteResponse,
						To: m.From,
						From: r.id,
						Term: r.Term,
						Reject: R,
					})
			}	
		}
	
	}
case StateCandidate:{
	switch m.MsgType {
		case pb.MessageType_MsgAppend:{
			if m.Term >= r.Term{
				r.Term = m.Term
				r.becomeFollower(m.Term,m.From)
			}
			r.handleAppendEntries(m)
		}
		case pb.MessageType_MsgHup:{                //候选人开始竞选
			if r.id == r.Vote{                                //如果候选人平票则通过增加任期来进行下一次选举
				r.Term++
			}
			r.votes[r.id] = true                          //对自己投票
			r.Vote = r.id                                   //对下次是否平票做判断
			for i := range r.votes{                     //向除了自己的结点号发起投票
				if i != r.id{	
					r.msgs = append(r.msgs, pb.Message{
						MsgType: pb.MessageType_MsgRequestVote,
						To: i,
						From: r.id,
						Term: r.Term,
					})
				}
		    }
			r.Step(pb.Message{
				To: r.id,
				From: r.id,
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				Term: r.Term,
				Reject: false,
			})                                     
		}
    case pb.MessageType_MsgHeartbeat:{
		if m.Term > r.Term{
			r.becomeFollower(m.Term,m.From)
			r.Term = m.Term
		}
	}


	case pb.MessageType_MsgRequestVoteResponse:{        //对投票统计
		sum := 0
		plen := len(r.votes)
		if !m.Reject {
			r.votes[m.From] = true
		}

		for i := range r.votes{
			if r.votes[i] {
				sum++
			}
		}
		if plen%2 == 1{
			if sum > plen/2 {
				r.electionElapsed = 0
				r.becomeLeader()
				r.Step(pb.Message{
					MsgType: pb.MessageType_MsgBeat,
				})	
			}
		}else{
			if sum >= plen/2 {
				r.electionElapsed = 0
				r.becomeLeader()
				r.Step(pb.Message{
					MsgType: pb.MessageType_MsgBeat,
				})
			}
		}
	}

	

		case pb.MessageType_MsgRequestVote:{      //收到投票请求
			R := true
			if m.Term > r.Term{                   //如果另一个候选者的term大于该候选者的term
				R = false
				r.becomeFollower(m.Term,m.From)
				r.Vote = m.From
			}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgRequestVoteResponse,
				To: m.From,
				From: r.id,
				Term: m.Term,
				Reject: R,
			})
		}
	}

}
	case StateLeader:{
		switch m.MsgType {
			case pb.MessageType_MsgAppend:{
				if m.Term > r.Term{
					r.Term = m.Term
					r.becomeFollower(m.Term,m.From)
					r.handleAppendEntries(m)
				}
			}
			
			case pb.MessageType_MsgBeat:{          //发送已经定义的心跳包
				for i := range r.votes {
					if i != r.id{
						r.sendHeartbeat(i)
					}
				}
			}
		case pb.MessageType_MsgPropose:{
			for i := range r.votes{
				if i != r.id{
					r.sendAppend(i)
				}
			}
		}
			
		}
	}
}
return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
