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
	"sync"

	"github.com/tiglabs/baudengine/util/log"
)

type Server struct {
	config    *Config
	raftStore *RaftStore
	wg        sync.WaitGroup

	apiServer *ApiServer

	idGenerator IDGenerator
}

func NewServer() *Server {
	return new(Server)
}

func (server *Server) Start(config *Config) (err error) {
	server.config = config
	server.raftStore, err = NewRaftStore(config)
	if err != nil {
		log.Error("fail to create raft store. err:[%v]", err)
		server.Shutdown()
		return err
	}

	server.apiServer = NewApiServer(config, server)
	if err := server.apiServer.Start(); err != nil {
		log.Error("fail to start api server. err:[%v]", err)
		server.Shutdown()
		return err
	}
	server.watchLeader()
	return nil
}

func (server *Server) Shutdown() {
	if server.idGenerator != nil {
		server.idGenerator.Close()
		server.idGenerator = nil
	}
	if server.apiServer != nil {
		server.apiServer.Close()
		server.apiServer = nil
	}
	if server.raftStore != nil {
		server.raftStore.Stop()
		server.raftStore = nil
	}
	server.wg.Wait()
}

func (server *Server) watchLeader() {
	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			select {
			case leaderInfo, ok := <-server.raftStore.GetLeaderAsync():
				if !ok {
					log.Debug("closed leader watch channel")
					return
				}
				if leaderInfo.IsLeader {
					log.Debug("master watch leader run, I'm leader now.")
					if server.idGenerator == nil {
						server.idGenerator = GetIdGeneratorSingle(server.raftStore)
					}
					ApiServerRunning = true
				} else {
					log.Debug("master watch leader run, I'm not leader now.")
					ApiServerRunning = false
					if server.idGenerator != nil {
						server.idGenerator.Close()
						server.idGenerator = nil
					}
				}
			}
		}
	}()
}
