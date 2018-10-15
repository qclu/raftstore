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
	"encoding/json"
	"fmt"
	"github.com/tiglabs/baudengine/proto"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/raft/jepsenraft/raftstore"
	"sync"
	"sync/atomic"
)

var (
	GEN_STEP          uint32 = 100
	AUTO_INCREMENT_ID        = fmt.Sprintf("$auto_increment_id")

	idGeneratorSingle     IDGenerator
	idGeneratorSingleLock sync.Mutex
	idGeneratorSingleDone uint32
)

type IDGenerator interface {
	GenID() (uint32, error)
	Close()
}

func GetIdGeneratorSingle(rs *RaftStore) IDGenerator {
	if idGeneratorSingle != nil {
		return idGeneratorSingle
	}
	if atomic.LoadUint32(&idGeneratorSingleDone) == 1 {
		return idGeneratorSingle
	}

	idGeneratorSingleLock.Lock()
	defer idGeneratorSingleLock.Unlock()

	if atomic.LoadUint32(&idGeneratorSingleDone) == 0 {
		if rs == nil {
			log.Error("store should not be nil at first time when create IdGenerator single")
			return nil
		}
		idGeneratorSingle = NewIDGenerator([]byte(AUTO_INCREMENT_ID), GEN_STEP, rs)
		atomic.StoreUint32(&idGeneratorSingleDone, 1)

		log.Info("IdGenerator single has started")
	}

	return idGeneratorSingle
}

type StoreIdGenerator struct {
	lock sync.Mutex
	base uint32
	end  uint32

	key  []byte
	step uint32

	rs *RaftStore
}

func NewIDGenerator(key []byte, step uint32, rstore *RaftStore) *StoreIdGenerator {
	return &StoreIdGenerator{key: key, step: step, rs: rstore}
}

func (id *StoreIdGenerator) GenID() (uint32, error) {
	if id == nil {
		return 0, pkg.ErrInternalError
	}

	if id.base == id.end {
		id.lock.Lock()

		if id.base == id.end {
			log.Debug("[GENID] before generate!!!!!! (base %d, end %d)", id.base, id.end)
			end, err := id.generate()
			if err != nil {
				id.lock.Unlock()
				return 0, err
			}

			id.end = end
			id.base = id.end - id.step
			log.Debug("[GENID] after generate!!!!!! (base %d, end %d)", id.base, id.end)
		}

		id.lock.Unlock()
	}

	atomic.AddUint32(&id.base, 1)

	return id.base, nil
}

func (id *StoreIdGenerator) Close() {
	if id == nil {
		return
	}

	idGeneratorSingleLock.Lock()
	defer idGeneratorSingleLock.Unlock()

	idGeneratorSingle = nil
	atomic.StoreUint32(&idGeneratorSingleDone, 0)

	log.Info("IdGenerator single has closed")
}

func (id *StoreIdGenerator) get(key string) ([]byte, error) {
	value, err := id.rs.fsm.Store.Get(key)
	if err != nil {
		return nil, err
	}
	return value.([]byte), nil
}

func (id *StoreIdGenerator) put(key string, value []byte) error {
	raftKvData := &raftstore.RaftKvData{
		Op: CMD_OP_PUT,
		K:  key,
		V:  value,
	}
	cmd, err := json.Marshal(raftKvData)
	if err != nil {
		log.Error("fail to marshal raftKvData[%v]. err:[%v]", raftKvData, err)
		return err
	}
	_, err = id.rs.raftPartition.Submit(cmd)
	if err != nil {
		log.Error("fail to put id[%v] into store. err:[%v]", value, err)
		return err
	}

	return nil
}

func (id *StoreIdGenerator) generate() (uint32, error) {
	value, err := id.get(string(id.key))
	if err != nil {
		return 0, err
	}

	var end uint32
	if len(value) != 0 {
		end = util.BytesToUint32(value)
	}
	end += id.step
	value = util.Uint32ToBytes(end)
	err = id.put(string(id.key), value)
	if err != nil {
		return 0, err
	}

	return end, nil
}
