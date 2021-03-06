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
	"fmt"
	"github.com/tecbot/gorocksdb"
)

//#cgo CFLAGS:-I/usr/local/include
//#cgo LDFLAGS:-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy
import "C"

type RocksDBStore struct {
	dir string
	db  *gorocksdb.DB
}

func NewRocksDBStore(dir string) (store *RocksDBStore) {
	store = &RocksDBStore{dir: dir}
	err := store.Open()
	if err != nil {
		panic(fmt.Sprintf("Failed to Open rocksDB! err:%v", err.Error()))
	}
	return store
}

func (rs *RocksDBStore) Open() error {
	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	db, err := gorocksdb.OpenDb(opts, rs.dir)
	if err != nil {
		err = fmt.Errorf("action[openRocksDB],err:%v", err)
		return err
	}
	rs.db = db

	return nil

}

func (rs *RocksDBStore) Close() error {
	if rs.db != nil {
		rs.db.Close()
		rs.db = nil
	}
	return nil

}

func (rs *RocksDBStore) Del(key interface{}) (result interface{}, err error) {
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(true)
	defer wb.Clear()
	slice, err := rs.db.Get(ro, []byte(key.(string)))
	if err != nil {
		return
	}
	result = slice.Data()
	err = rs.db.Delete(wo, []byte(key.(string)))
	return
}

func (rs *RocksDBStore) Put(key, value interface{}) (result interface{}, err error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(true)
	wb.Put([]byte(key.(string)), value.([]byte))
	if err := rs.db.Write(wo, wb); err != nil {
		return nil, err
	}
	result = value
	return result, nil
}

func (rs *RocksDBStore) Get(key interface{}) (result interface{}, err error) {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	value, err := rs.db.GetBytes(ro, []byte(key.(string)))
	if err != nil {
		return nil, err
	}
	valueByte := make([]byte, len(value))
	copy(valueByte, value)
	return valueByte, nil
}