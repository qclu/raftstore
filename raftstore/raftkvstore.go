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

package raftstore

import (
	"encoding/json"
	"fmt"
	"github.com/tecbot/gorocksdb"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
	"io"
	"strconv"
)

const (
	RaftApplyId = "RaftApplyId"
)

type RaftKvData struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

type RaftKvFsm struct {
	Store               *RocksDBStore
	Applied             uint64
	LeaderChangeHandler RaftLeaderChangeHandler
	PeerChangeHandler   RaftPeerChangeHandler
	ApplyHandler        RaftKvApplyHandler
}

type RaftLeaderChangeHandler func(leader uint64)
type RaftPeerChangeHandler func(confChange *proto.ConfChange) (err error)
type RaftKvApplyHandler func(cmd *RaftKvData) (err error)

//need application to implement -- begin
/*
type MasterRaftStoreConfig struct {
	peers     []PeerAddress
	peerAddrs []string
}

type MasterRaftStore struct {
	nodeId        uint64
	groupId       uint64
	ip            string
	heartbeatPort int
	replicatePort int
	walDir        string
	storeDir      string
	config        *MasterRaftStoreConfig
	raftStore     RaftStore
	fsm           *RaftKvFsm
	raftPartition Partition
	wg            sync.WaitGroup
}

func (m *MasterRaftStore) handleLeaderChange(leader uint64) {
	fmt.Println("leader change leader ", leader)
	return
}

func (m *MasterRaftStore) handlePeerChange(confChange *proto.ConfChange) (err error) {
	fmt.Println("peer change confChange ", confChange)
	return nil
}

func (m *MasterRaftStore) handleApply(cmd *RaftKvData) (err error) {
	fmt.Println("apply cmd ", cmd)
	return nil
}
*/
//need application to implement -- end

//example begin ----------------------------------
//just for test
/*
type testSM struct {
	stopc chan struct{}
}

func (m *MasterRaftStore) handleFunctions() {
	//	http.Handle("/raftNode/add", m.handlerWithInterceptor())
	//	http.Handle("/raftNode/remove", m.handlerWithInterceptor())
	http.Handle("/raftKvTest/submit", m.handlerWithInterceptor())
	return
}

func (m *MasterRaftStore) handlerWithInterceptor() http.Handler {
	return http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if m.raftPartition.IsLeader() {
				m.ServeHTTP(w, r)
			} else {
				http.Error(w, "not leader", http.StatusForbidden)
			}
		})
}

func (m *MasterRaftStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	//	case "/raftNode/add":
	//		m.handleAddRaftNode(w, r)
	//	case "/raftNode/remove":
	//		m.handleRemoveRaftNode(w, r)
	case "/raftKvTest/submit":
		m.handleRaftKvSubmit(w, r)
	default:
	}
}

func (m *MasterRaftStore) handleRaftKvSubmit(w http.ResponseWriter, r *http.Request) {
	var (
		cmd []byte
		err error
	)
	raftKvData := new(RaftKvData)
	raftKvData.Op = uint32(0x01)
	raftKvData.K = "raft_kv_test01"
	value := strconv.FormatUint(uint64(1234), 10)
	raftKvData.V = []byte(value)
	cmd, err = json.Marshal(raftKvData)
	if err != nil {
		goto errDeal
	}
	if _, err = m.raftPartition.Submit(cmd); err != nil {
		goto errDeal
	}
	fmt.Println("raft kv submit", raftKvData, cmd)
	return
errDeal:
	log.Error("action[submit] err:%v", err.Error())
	return
}

func main() {
	fmt.Println("Hello raft kv store")
	var (
		confFile  = flag.String("c", "", "config file path")
		testParam testSM
	)

	flag.Parse()
	cfg := config.LoadConfigFile(*confFile)
	nodeId := cfg.GetString("nodeid")
	heartbeatPort := cfg.GetString("heartbeat")
	replicatePort := cfg.GetString("replicate")

	m := new(MasterRaftStore)
	m.config = new(MasterRaftStoreConfig)
	m.nodeId, _ = strconv.ParseUint(nodeId, 10, 10)
	m.heartbeatPort, _ = strconv.Atoi(heartbeatPort)
	m.replicatePort, _ = strconv.Atoi(replicatePort)
	m.groupId = 1
	m.walDir = "raft_log"
	m.storeDir = "store_log"

	peerAddrs := cfg.GetString("peers")
	if err := m.parsePeers(peerAddrs); err != nil {
		log.Fatal("parse peers fail", err)
		return
	}

	err := m.CreateKvRaft()
	if err != nil {
		fmt.Println("creade kv raft err ", err)
		return
	}

	go func() {
		m.handleFunctions()
		err := http.ListenAndServe(":8800", nil)
		if err != nil {
			fmt.Println("listenAndServe", err)
		}
	}()

	for {
		select {
		case <-testParam.stopc:
			return
		default:
		}
	}

	return
}

func (m *MasterRaftStore) parsePeers(peerStr string) error {
	fmt.Printf("peerStr %s", peerStr)
	peerArr := strings.Split(peerStr, ",")

	m.config.peerAddrs = peerArr
	for _, peerAddr := range peerArr {
		peer := strings.Split(peerAddr, ":")
		id, err := strconv.ParseUint(peer[0], 10, 64)
		if err != nil {
			return err
		}
		ip := peer[1]

		raftPeer := PeerAddress{
			Peer: raftproto.Peer{
				ID: id,
			},
			Address:       ip,
			HeartbeatPort: m.heartbeatPort,
			ReplicatePort: m.replicatePort,
		}

		m.config.peers = append(m.config.peers, raftPeer)
	}

	return nil
}

func (m *MasterRaftStore) CreateKvRaft() (err error) {
	raftCfg := &Config{
		NodeID:        m.nodeId,
		WalPath:       m.walDir,
		IpAddr:        m.ip,
		HeartbeatPort: m.heartbeatPort,
		ReplicatePort: m.replicatePort,
	}

	fmt.Println("create kv raft ", raftCfg.HeartbeatPort, raftCfg.ReplicatePort)

	if m.raftStore, err = NewRaftStore(raftCfg);
		err != nil {
		return errors.Annotatef(err, "NewRaftStore failed! id[%v] walPath[%v]", m.nodeId, m.walDir)
	}

	fsm := new(RaftKvFsm)
	fsm.store = NewRocksDBStore(m.walDir)
	fsm.RegisterLeaderChangeHandler(m.handleLeaderChange)
	fsm.RegisterPeerChangeHandler(m.handlePeerChange)
	fsm.RegisterApplyHandler(m.handleApply)
	fsm.restore()

	m.fsm = fsm
	fmt.Println(m.config.peers)

	partitionCfg := &PartitionConfig{
		ID:      m.groupId,
		Peers:   m.config.peers,
		Applied: fsm.applied,
		SM:      fsm,
	}

	if m.raftPartition, err = m.raftStore.CreatePartition(partitionCfg); err != nil {
		return errors.Annotate(err, "CreatePartition failed")
	}

	return
}

//example end -------------------------------------------
*/

//Handler
func (rkf *RaftKvFsm) RegisterLeaderChangeHandler(handler RaftLeaderChangeHandler) {
	rkf.LeaderChangeHandler = handler
}

func (rkf *RaftKvFsm) RegisterPeerChangeHandler(handler RaftPeerChangeHandler) {
	rkf.PeerChangeHandler = handler
}

func (rkf *RaftKvFsm) RegisterApplyHandler(handler RaftKvApplyHandler) {
	rkf.ApplyHandler = handler
}

//restore apply id
func (rkf *RaftKvFsm) Restore() {
	rkf.restoreApplied()
}

func (rkf *RaftKvFsm) restoreApplied() {
	value, err := rkf.Get(RaftApplyId)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied err:%v", err.Error()))
	}
	byteValues := value.([]byte)
	if len(byteValues) == 0 {
		rkf.Applied = 0
		return
	}
	applied, err := strconv.ParseUint(string(byteValues), 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Failed to restore applied,err:%v ", err.Error()))
	}
	rkf.Applied = applied
}

//raft StateMachine
func (rkf *RaftKvFsm) Apply(command []byte, index uint64) (resp interface{}, err error) {
	if rkf.ApplyHandler != nil {
		cmd := new(RaftKvData)
		if err = json.Unmarshal(command, cmd); err != nil {
			return nil, fmt.Errorf("action[fsmApply],unmarshal data:%v, err:%v", command, err.Error())
		}
		err = rkf.ApplyHandler(cmd)
		//store applied index
		rkf.Store.Put(RaftApplyId, []byte(strconv.FormatUint(uint64(index), 10)))
		rkf.Applied = index
	}
	return
}

func (rkf *RaftKvFsm) ApplyMemberChange(confChange *proto.ConfChange, index uint64) (resp interface{}, err error) {
	if rkf.PeerChangeHandler != nil {
		err = rkf.PeerChangeHandler(confChange)
	}
	return
}

func (rkf *RaftKvFsm) Snapshot() (proto.Snapshot, error) {
	snapshot := rkf.Store.RocksDBSnapshot()

	iterator := rkf.Store.Iterator(snapshot)
	iterator.SeekToFirst()
	return &RaftKvSnapshot{
		applied:   rkf.Applied,
		snapshot:  snapshot,
		raftKvFsm: rkf,
		iterator:  iterator,
	}, nil
}

func (rkf *RaftKvFsm) ApplySnapshot(peers []proto.Peer, iterator proto.SnapIterator) (err error) {
	var data []byte
	for err == nil {
		if data, err = iterator.Next(); err != nil {
			goto errDeal
		}
		cmd := &RaftKvData{}
		if err = json.Unmarshal(data, cmd); err != nil {
			goto errDeal
		}
		if _, err = rkf.Store.Put(cmd.K, cmd.V); err != nil {
			goto errDeal
		}

		if err = rkf.ApplyHandler(cmd); err != nil {
			goto errDeal
		}
	}
	return
errDeal:
	if err == io.EOF {
		return
	}
	log.Error(fmt.Sprintf("action[ApplySnapshot] failed,err:%v", err.Error()))
	return err
}

func (rkf *RaftKvFsm) HandleFatalEvent(err *raft.FatalError) {
	panic(err.Err)
}

func (rkf *RaftKvFsm) HandleLeaderChange(leader uint64) {
	if rkf.LeaderChangeHandler != nil {
		go rkf.LeaderChangeHandler(leader)
	}
}

//snapshot interface
type RaftKvSnapshot struct {
	raftKvFsm *RaftKvFsm
	applied   uint64
	snapshot  *gorocksdb.Snapshot
	iterator  *gorocksdb.Iterator
}

func (rks *RaftKvSnapshot) ApplyIndex() uint64 {
	return rks.applied
}

func (rks *RaftKvSnapshot) Close() {
	rks.raftKvFsm.Store.ReleaseSnapshot(rks.snapshot)
}

func (rks *RaftKvSnapshot) Next() (data []byte, err error) {
	if rks.iterator.Valid() {
		rks.iterator.Next()
		return data, nil
	}
	return nil, io.EOF
}

type Store interface {
	Put(key, val interface{}) (interface{}, error)
	BatchPut(cmdMap map[string][]byte) (err error)
	Get(key interface{}) (interface{}, error)
	Seek(prefix interface{}) (interface{}, error)
	Del(key interface{}) (interface{}, error)
	BatchDel(cmdMap map[string][]byte) (err error)
}

//store interface
func (rkf *RaftKvFsm) Put(key, val interface{}) (interface{}, error) {
	return rkf.Store.Put(key, val)
}

func (rkf *RaftKvFsm) BatchPut(cmdMap map[string][]byte) (err error) {
	return
}

func (rkf *RaftKvFsm) Get(key interface{}) (interface{}, error) {
	return rkf.Store.Get(key)
}

func (rkf *RaftKvFsm) Seek(prefix interface{}) (interface{}, error) {
	return nil, nil
}

func (rkf *RaftKvFsm) Del(key interface{}) (interface{}, error) {
	return rkf.Store.Del(key)
}

func (rkf *RaftKvFsm) BatchDel(cmdMap map[string][]byte) (err error) {
	return
}
