// Copyright 2018 The ChuBao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package raftserver

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft/jepsenraft/raftstore"
	"github.com/tiglabs/raft/proto"
	"hash/crc32"
	"regexp"
	"strconv"
)

const (
	CMD_OP_PUT uint32 = 0x01
	CMD_OP_DEL uint32 = 0x02
)

type LeaderInfo struct {
	IsLeader      bool
	NewLeaderId   uint64
	NewLeaderAddr string
}

type RaftStoreConfig struct {
	peers []raftstore.PeerAddress
}

type RaftStore struct {
	nodeId        uint64
	groupId       uint64
	ip            string
	heartbeatPort int
	replicatePort int
	walDir        string
	storeDir      string
	rsConfig      *RaftStoreConfig
	config        *Config

	leaderChangingCh chan *LeaderInfo
	leaderInfo       *LeaderInfo

	raftStore     raftstore.RaftStore
	fsm           *raftstore.RaftKvFsm
	raftPartition raftstore.Partition

	wg sync.WaitGroup
}

func NewRaftStore(config *Config) (*RaftStore, error) {
	rs := new(RaftStore)
	rs.config = config
	rs.nodeId = config.ClusterCfg.CurNode.NodeId
	rs.ip = config.ClusterCfg.CurNode.Host
	rs.heartbeatPort = int(config.ClusterCfg.CurNode.RaftHeartbeatPort)
	rs.replicatePort = int(config.ClusterCfg.CurNode.RaftReplicatePort)
	rs.groupId = CalcGroupID(config.ClusterCfg.ClusterID)
	raftDataDir := filepath.Join(config.ModuleCfg.DataPath, "raft_log")
	if err := os.MkdirAll(raftDataDir, 0755); err != nil {
		log.Error("make raft data root directory[%v] failed, err[%v]", raftDataDir, err)
		return nil, err
	}
	rs.walDir = raftDataDir
	storeDataDir := filepath.Join(config.ModuleCfg.DataPath, "store_log")
	if err := os.MkdirAll(storeDataDir, 0755); err != nil {
		log.Error("make store data root directory[%v] failed, err[%v]", storeDataDir, err)
		return nil, err
	}
	rs.storeDir = storeDataDir
	rs.rsConfig = new(RaftStoreConfig)
	for _, node := range config.ClusterCfg.Nodes {
		raftPeer := raftstore.PeerAddress{
			Peer: proto.Peer{
				ID: node.NodeId,
			},
			Address:       node.Host,
			HeartbeatPort: int(node.RaftHeartbeatPort),
			ReplicatePort: int(node.RaftReplicatePort),
		}
		rs.rsConfig.peers = append(rs.rsConfig.peers, raftPeer)
	}
	rs.leaderChangingCh = make(chan *LeaderInfo, 4)

	err := rs.CreateKvRaft()
	if err != nil {
		log.Error("creade kv raft err ", err)
		return nil, err
	}
	return rs, nil
}

func CalcGroupID(clusterID string) uint64 {
	digitRegExp := regexp.MustCompile(`[0-9]+`)
	groupID, err := strconv.ParseUint(digitRegExp.FindString(clusterID), 10, 64)
	if err == nil {
		return groupID
	} else {
		return uint64(crc32.ChecksumIEEE([]byte(clusterID)))
	}
}

func (rs *RaftStore) CreateKvRaft() (err error) {
	raftCfg := &raftstore.Config{
		NodeID:        rs.nodeId,
		WalPath:       rs.walDir,
		IpAddr:        rs.ip,
		HeartbeatPort: rs.heartbeatPort,
		ReplicatePort: rs.replicatePort,
	}
	log.Debug("create kv raft, raftCfg:[%v]", raftCfg)
	rs.raftStore, err = raftstore.NewRaftStore(raftCfg)
	if err != nil {
		return err
	}
	fsm := new(raftstore.RaftKvFsm)
	fsm.Store = raftstore.NewRocksDBStore(rs.walDir)
	fsm.RegisterLeaderChangeHandler(rs.handleLeaderChange)
	fsm.RegisterPeerChangeHandler(rs.handlePeerChange)
	fsm.RegisterApplyHandler(rs.handleApply)
	fsm.Restore()
	rs.fsm = fsm
	partitionCfg := &raftstore.PartitionConfig{
		ID:      rs.groupId,
		Applied: fsm.Applied,
		Peers:   rs.rsConfig.peers,
		SM:      fsm,
	}
	rs.raftPartition, err = rs.raftStore.CreatePartition(partitionCfg)
	if err != nil {
		return err
	}
	return nil
}

func (rs *RaftStore) Stop() {
	if rs != nil {
		if rs.raftStore != nil {
			rs.raftStore.Stop()
			rs.raftStore = nil
		}
		if rs.fsm.Store != nil {
			rs.fsm.Store.Close()
			rs.fsm.Store = nil
		}
	}
}

func (rs *RaftStore) isLeader(newLeaderId uint64) bool {
	return newLeaderId != 0 && rs.config.ClusterCfg.CurNodeId == newLeaderId
}

func (rs *RaftStore) GetLeaderAsync() <-chan *LeaderInfo {
	return rs.leaderChangingCh
}

func (rs *RaftStore) GetLeaderSync() *LeaderInfo {
	log.Debug("rs: %p", rs)
	return rs.leaderInfo
}

func (rs *RaftStore) handleLeaderChange(leaderId uint64) {
	log.Info("raft leader had changed to id[%v]", leaderId)

	var leaderNode *ClusterNode
	var leaderAddr string
	for _, node := range rs.config.ClusterCfg.Nodes {
		if node.NodeId == leaderId {
			leaderNode = node
			break
		}
	}
	if leaderNode != nil {
		leaderAddr = util.BuildAddr(leaderNode.Host, leaderNode.HttpPort)
	}

	info := &LeaderInfo{
		IsLeader:      rs.isLeader(leaderId),
		NewLeaderId:   leaderId,
		NewLeaderAddr: leaderAddr,
	}
	rs.leaderInfo = info
	log.Debug("leader info: [%v]", info)
	log.Debug("rs: %p", rs)
	log.Debug("******************************************")
	select {
	case <-time.After(500 * time.Millisecond):
		log.Error("notify leader change timeout")
	case rs.leaderChangingCh <- info:
		log.Debug("notify leader change[%v] end", info)
	}
}

func (rs *RaftStore) handlePeerChange(confChange *proto.ConfChange) (err error) {
	var msg string
	addr := string(confChange.Context)
	switch confChange.Type {
	case proto.ConfAddNode:
		var arr []string
		if arr = strings.Split(addr, ":"); len(arr) < 2 {
			msg = fmt.Sprintf("action[handlePeerChange] nodeAddr[%v] is invalid", addr)
			break
		}
		rs.raftStore.AddNode(confChange.Peer.ID, arr[0])
		msg = fmt.Sprintf("peerID:%v,nodeAddr[%v] has been add", confChange.Peer.ID, addr)
	case proto.ConfRemoveNode:
		rs.raftStore.DeleteNode(confChange.Peer.ID)
		msg = fmt.Sprintf("peerID:%v,nodeAddr[%v] has been removed", confChange.Peer.ID, addr)
	}
	log.Warn("handlePeerChange invoked, msg:[%s]", msg)
	return nil
}

func (rs *RaftStore) handleApply(cmd *raftstore.RaftKvData) (err error) {
	switch cmd.Op {
	case CMD_OP_PUT:
		_, err := rs.fsm.Put(cmd.K, cmd.V)
		if err != nil {
			log.Error("rs.fsm.Store.Put err:[%v], key:[%s], value:[%v]", err, cmd.K, cmd.V)
		}
	case CMD_OP_DEL:
		_, err := rs.fsm.Del(cmd.K)
		if err != nil {
			log.Error("rs.fsm.Store.Del err:[%v], key:[%s]", err, cmd.K)
		}
	}
	return nil
}
