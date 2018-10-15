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
	"os"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
)

const DEFAULT_MASTER_CONFIG = `
# Master Configuration.

[module]
name = "baudengine master"
role = "master"
version = "v1"
# web request request signature key
signkey = ""
data-path = "/tmp/baudengine/master1/data"
use-ump = false
pprof = 10088

[log]
log-path = "/tmp/baudengine/master1/log"
#debug, info, warn, error
level="debug"
#debug, info, warn
raft-level= "debug"

[cluster]
cluster-id="1"
node-id=1
raft-heartbeat-interval=500
raft-retain-logs-count=100
fixed-replica-num=3

[[cluster.nodes]]
node-id=1
host="127.0.0.1"
http-port=8817
raft-heartbeat-port=8816
raft-replicate-port=8815

[[cluster.nodes]]
node-id=2
host="127.0.0.1"
http-port=8827
raft-heartbeat-port=8826
raft-replicate-port=8825

[[cluster.nodes]]
node-id=3
host="127.0.0.1"
http-port=8837
raft-heartbeat-port=8836
raft-replicate-port=8835

[ps]
heartbeat-interval=5000
raft-heartbeat-interval=100
raft-retain-logs=10000
raft-replica-concurrency=1
raft-snapshot-concurrency=1
`

const (
	CONFIG_LOG_LEVEL_DEBUG = "debug"
	CONFIG_LOG_LEVEL_INFO  = "info"
	CONFIG_LOG_LEVEL_WARN  = "warn"
	CONFIG_LOG_LEVEL_ERROR = "error"

	CONFIG_LOG_LEVEL_INT_DEBUG = 1
	CONFIG_LOG_LEVEL_INT_INFO  = 2
	CONFIG_LOG_LEVEL_INT_WARN  = 3
	CONFIG_LOG_LEVEL_INT_ERROR = 4
)

type Config struct {
	ModuleCfg  ModuleConfig  `toml:"module,omitempty" json:"module"`
	LogCfg     LogConfig     `toml:"log,omitempty" json:"log"`
	ClusterCfg ClusterConfig `toml:"cluster,omitempty" json:"cluster"`
}

func NewConfig(path string) *Config {
	c := new(Config)
	if _, err := toml.Decode(DEFAULT_MASTER_CONFIG, c); err != nil {
		log.Panic("fail to decode default config, err[%v]", err)
	}
	if len(path) != 0 {
		_, err := toml.DecodeFile(path, c)
		if err != nil {
			log.Panic("fail to decode config file[%v]. err[%v]", path, err)
		}
	}
	c.adjust()
	return c
}

func (c *Config) adjust() {
	c.ModuleCfg.adjust()
	c.LogCfg.adjust()
	c.ClusterCfg.adjust()
}

type ModuleConfig struct {
	StoreType string `toml:"store-type,omitempty" json:"store-type"`
	DataPath string `toml:"data-path,omitempty" json:"data-path"`
}

func (cfg *ModuleConfig) adjust() {
	adjustString(&cfg.DataPath, "no data path")
	_, err := os.Stat(cfg.DataPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.DataPath, os.ModePerm); err != nil {
			log.Panic("fail to create meta data path[%v]. err[%v]", cfg.DataPath, err)
		}
	}
}

type ClusterNode struct {
	NodeId            uint64 `toml:"node-id,omitempty" json:"node-id"`
	Host              string `toml:"host,omitempty" json:"host"`
	HttpPort          uint32 `toml:"http-port,omitempty" json:"http-port"`
	RaftHeartbeatPort uint32 `toml:"raft-heartbeat-port,omitempty" json:"raft-heartbeat-port"`
	RaftReplicatePort uint32 `toml:"raft-replicate-port,omitempty" json:"raft-replicate-port"`
}

type ClusterConfig struct {
	ClusterID             string         `toml:"cluster-id,omitempty" json:"cluster-id"`
	CurNodeId             uint64         `toml:"node-id,omitempty" json:"node-id"`
	HttpPort              uint32         `toml:"http-port,omitempty" json:"http-port"`
	RaftHeartbeatInterval uint64         `toml:"raft-heartbeat-interval,omitempty" json:"raft-heartbeat-interval"`
	RaftRetainLogsCount   uint64         `toml:"raft-retain-logs-count,omitempty" json:"raft-retain-logs-count"`
	FixedReplicaNum       uint64         `toml:"fixed-replica-num,omitempty" json:"fixed-replica-num"`
	Nodes                 []*ClusterNode `toml:"nodes,omitempty" json:"nodes"`
	CurNode               *ClusterNode
}

func (cfg *ClusterConfig) adjust() {
	adjustString(&cfg.ClusterID, "no cluster-id")
	adjustUint64(&cfg.CurNodeId, "no current node-id")
	adjustUint64(&cfg.RaftHeartbeatInterval, "no raft heartbeat interval")
	adjustUint64(&cfg.RaftRetainLogsCount, "no raft retain log count")
	adjustUint64(&cfg.FixedReplicaNum, "no fixed replica num")

	if len(cfg.Nodes) == 0 {
		log.Panic("cluster nodes is empty")
	}

	// validate whether is node-id duplicated
	tempNodes := make(map[uint64]*ClusterNode)

	for _, node := range cfg.Nodes {
		adjustUint64(&node.NodeId, "no node-id")
		adjustString(&node.Host, "no node host")
		adjustUint32(&node.HttpPort, "no node http port")
		if node.HttpPort <= 1024 || node.HttpPort > 65535 {
			log.Panic("out of node http port %d", node.HttpPort)
		}
		adjustUint32(&node.RaftHeartbeatPort, "no node raft heartbeat port")
		if node.RaftHeartbeatPort <= 1024 || node.RaftHeartbeatPort > 65535 {
			log.Panic("out of node raft heartbeat port %d", node.RaftHeartbeatPort)
		}
		adjustUint32(&node.RaftReplicatePort, "no node raft replicate port")
		if node.RaftReplicatePort <= 1024 || node.RaftReplicatePort > 65535 {
			log.Panic("out of node raft replicate port %d", node.RaftReplicatePort)
		}
		if _, ok := tempNodes[node.NodeId]; ok {
			log.Panic("duplicated node-id[%v]", node.NodeId)
		}
		tempNodes[node.NodeId] = node
		if node.NodeId == cfg.CurNodeId {
			cfg.CurNode = node
		}
	}
}

type LogConfig struct {
	LogPath   string `toml:"log-path,omitempty" json:"log-path"`
	Level     string `toml:"level,omitempty" json:"level"`
	RaftLevel string `toml:"raft-level,omitempty" json:"raft-level"`
}

func (c *LogConfig) adjust() {
	adjustString(&c.LogPath, "no log path")
	_, err := os.Stat(c.LogPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(c.LogPath, os.ModePerm); err != nil {
			log.Panic("fail to create log path[%v]. err[%v]", c.LogPath, err)
		}
	}
	adjustString(&c.Level, "no log level")
	c.Level = strings.ToLower(c.Level)
	switch c.Level {
	case CONFIG_LOG_LEVEL_DEBUG:
	case CONFIG_LOG_LEVEL_INFO:
	case CONFIG_LOG_LEVEL_WARN:
	case CONFIG_LOG_LEVEL_ERROR:
	default:
		log.Panic("Invalid log level[%v]", c.Level)
	}
	adjustString(&c.RaftLevel, "no raft log level")
	switch c.RaftLevel {
	case CONFIG_LOG_LEVEL_DEBUG:
	case CONFIG_LOG_LEVEL_INFO:
	case CONFIG_LOG_LEVEL_WARN:
	default:
		log.Panic("Invalid raft log level[%v]", c.RaftLevel)
	}
}

func adjustString(v *string, errMsg string) {
	if len(*v) == 0 {
		log.Panic("Config adjust string error, %v", errMsg)
	}
}

func adjustUint32(v *uint32, errMsg string) {
	if *v == 0 {
		log.Panic("Config adjust uint32 error, %v", errMsg)
	}
}

func adjustUint64(v *uint64, errMsg string) {
	if *v == 0 {
		log.Panic("Config adjust uint64 error, %v", errMsg)
	}
}

func adjustDuration(v *util.Duration, errMsg string) {
	if v.Duration == 0 {
		log.Panic("Config adjust duration error, %v", errMsg)
	}
}
