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
	"math"
	"net/http"
	"net/http/httputil"
	"strconv"
	"sync"
	"time"

	"github.com/tiglabs/baudengine/proto"
	"github.com/tiglabs/baudengine/util"
	"github.com/tiglabs/baudengine/util/log"
	"github.com/tiglabs/baudengine/util/netutil"
	"github.com/tiglabs/raft/jepsenraft/raftstore"
)

const (
	DEFAULT_CONN_LIMIT    = 100
	DEFAULT_CLOSE_TIMEOUT = 5 * time.Second

	OBJ_ID  = "obj_id"
	OBJ_VAL = "obj_val"

	OBJ_CREATE = "/obj/create"
	OBJ_DELETE = "/obj/delete"
	OBJ_UPDATE = "/obj/update"
	OBJ_GET    = "/obj/get"
)

var ApiServerRunning = false

type ApiServer struct {
	config     *Config
	httpServer *netutil.Server
	wg         sync.WaitGroup
	srv        *Server
}

func NewApiServer(config *Config, server *Server) *ApiServer {
	cfg := &netutil.ServerConfig{
		Name:         "api-server",
		Addr:         util.BuildAddr("0.0.0.0", config.ClusterCfg.CurNode.HttpPort),
		Version:      "v1",
		ConnLimit:    DEFAULT_CONN_LIMIT,
		CloseTimeout: DEFAULT_CLOSE_TIMEOUT,
	}

	apiServer := &ApiServer{
		config:     config,
		httpServer: netutil.NewServer(cfg),
		srv:        server,
	}
	apiServer.initAdminHandler()

	return apiServer
}

func (s *ApiServer) Start() error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		if err := s.httpServer.Run(); err != nil {
			log.Error("api server run error[%v]", err)
		}
	}()

	log.Info("ApiServer has started")
	return nil
}

func (s *ApiServer) Close() {
	if s.httpServer != nil {
		s.httpServer.Shutdown()
		s.httpServer = nil
	}
	s.wg.Wait()
	log.Info("ApiServer has closed")
}

func (s *ApiServer) initAdminHandler() {
	s.httpServer.Handle(http.MethodPost, OBJ_CREATE, s.handlerWithInterceptor())
	s.httpServer.Handle(http.MethodPost, OBJ_DELETE, s.handlerWithInterceptor())
	s.httpServer.Handle(http.MethodPut, OBJ_UPDATE, s.handlerWithInterceptor())
	s.httpServer.Handle(http.MethodGet, OBJ_GET, s.handlerWithInterceptor())
}

func (s *ApiServer) handlerWithInterceptor() netutil.Handle {
	return netutil.Handle(
		func(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
			err := s.checkLeader(w, r)
			if err != nil {
				return
			} else {
				if !ApiServerRunning {
					sendReply(w, newHttpErrReply(pkg.ErrNoMSLeader))
					return
				}
				s.ServeHTTP(w, r, params)
			}
		})
}

func (s *ApiServer) ServeHTTP(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	log.Debug("URL:%s, params:%v, request:[%v]", r.URL.Path, params, r)

	switch r.URL.Path {
	case OBJ_CREATE:
		s.handleObjCreate(w, r, params)
	case OBJ_GET:
		s.handleObjGet(w, r, params)
	default:
		log.Error("Not found route:[%s]", r.URL.Path)
	}
}

// ops
func (s *ApiServer) handleObjCreate(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	objId, err := checkMissingParam(w, r, OBJ_ID)
	if err != nil {
		return
	}
	objVal := r.FormValue(OBJ_VAL)
	err = s.createObj(objId, objVal)
	if err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	sendReply(w, newHttpSucReply(nil))
}

func (s *ApiServer) createObj(objId, objVal string) error {
	raftKvData := &raftstore.RaftKvData{
		Op: CMD_OP_PUT,
		K:  objId,
		V:  []byte(objVal),
	}
	cmd, err := json.Marshal(raftKvData)
	if err != nil {
		log.Error("fail to marshal raftKvData[%v]. err:[%v]", raftKvData, err)
		return err
	}
	log.Info("create : %v", raftKvData)
	_, err = s.srv.raftStore.raftPartition.Submit(cmd)
	if err != nil {
		log.Error("fail to put obj[%v] into store. err:[%v]", objId, err)
		return pkg.ErrLocalZoneOpsFailed
	}
	return nil
}

/*
func (s *ApiServer) handleObjDelete(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	objId, err := checkMissingParam(w, r, OBJ_ID)
	if err != nil {
		return
	}
	err = s.deleteObj(objId)
	if err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	sendReply(w, newHttpSucReply(nil))
}

func (s *ApiServer) deleteObj(objId string) error {
	raftKvData := &raftstore.RaftKvData{
		Op: CMD_OP_DEL,
		K:  objId,
	}
	cmd, err := json.Marshal(raftKvData)
	if err != nil {
		log.Error("fail to marshal raftKvData[%v]. err:[%v]", raftKvData, err)
		return err
	}
	_, err = s.srv.raftStore.raftPartition.Submit(cmd)
	if err != nil {
		log.Error("fail to delete obj[%v] from store. err:[%v]", objId, err)
		return pkg.ErrLocalZoneOpsFailed
	}
	s.srv.objCache.Delete(objId)
	return nil
}

func (s *ApiServer) handleObjUpdate(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	objId, err := checkMissingParam(w, r, OBJ_ID)
	if err != nil {
		return
	}
	objVal, err := checkMissingParam(w, r, OBJ_VAL)
	if err != nil {
		return
	}

	err = s.updateObj(objId, objVal)
	if err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	sendReply(w, newHttpSucReply(nil))
}

func (s *ApiServer) updateObj(objId, objVal string) error {
	_, ok := s.srv.objCache.Load(objId)
	if !ok {
		return errors.New("object not exist")
	}
	raftKvData := &raftstore.RaftKvData{
		Op: CMD_OP_PUT,
		K:  objId,
		V:  []byte(objVal),
	}
	cmd, err := json.Marshal(raftKvData)
	if err != nil {
		log.Error("fail to marshal raftKvData[%v]. err:[%v]", raftKvData, err)
		return err
	}
	_, err = s.srv.raftStore.raftPartition.Submit(cmd)
	if err != nil {
		log.Error("fail to update obj[%v] from store. err:[%v]", objId, err)
		return pkg.ErrLocalZoneOpsFailed
	}
	s.srv.objCache.Store(objId, objVal)
	return nil
}*/

func (s *ApiServer) handleObjGet(w http.ResponseWriter, r *http.Request, params netutil.UriParams) {
	objId, err := checkMissingParam(w, r, OBJ_ID)
	if err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}

	objVal, err := s.getObj(objId)
	if err != nil {
		sendReply(w, newHttpErrReply(err))
		return
	}
	sendReply(w, newHttpSucReply(objVal))
}

func (s *ApiServer) getObj(objId string) (objVal string, err error) {
	var result interface{}
	result, err = s.srv.raftStore.fsm.Store.Get(objId)
	if err != nil {
		return
	}
	objVal = string(result.([]byte))
	return
}

func newHttpSucReply(data interface{}) *netutil.HttpReply {
	return &netutil.HttpReply{
		Code: pkg.ERRCODE_SUCCESS,
		Msg:  pkg.ErrSuc.Error(),
		Data: data,
	}
}

func newHttpErrReply(err error) *netutil.HttpReply {
	if err == nil {
		return newHttpSucReply(nil)
	}

	code, ok := pkg.Err2CodeMap[err]
	if ok {
		return &netutil.HttpReply{
			Code: code,
			Msg:  err.Error(),
		}
	} else {
		return &netutil.HttpReply{
			Code: pkg.ERRCODE_INTERNAL_ERROR,
			Msg:  err.Error(),
		}
	}
}

func (s *ApiServer) checkLeader(w http.ResponseWriter, r *http.Request) error {
	leaderInfo := s.srv.raftStore.GetLeaderSync()

	if leaderInfo == nil {
		sendReply(w, newHttpErrReply(pkg.ErrNoMSLeader))
		return pkg.ErrNoMSLeader
	}

	if !leaderInfo.IsLeader {
		if leaderInfo.NewLeaderId == 0 {
			sendReply(w, newHttpErrReply(pkg.ErrNoMSLeader))
			return pkg.ErrNoMSLeader
		} else {
			proxy := &Proxy{targetHost: leaderInfo.NewLeaderAddr}
			proxy.proxy(w, r)
			return pkg.ErrSuc
		}
	}
	return nil
}

type Proxy struct {
	targetHost string
}

func (p *Proxy) proxy(w http.ResponseWriter, r *http.Request) {
	director := func(request *http.Request) {
		request.URL.Scheme = "http"
		request.URL.Host = p.targetHost
	}
	reverseProxy := &httputil.ReverseProxy{Director: director}
	reverseProxy.ServeHTTP(w, r)
}

func checkMissingParam(w http.ResponseWriter, r *http.Request, paramName string) (string, error) {
	paramVal := r.FormValue(paramName)
	if paramVal == "" {
		reply := newHttpErrReply(pkg.ErrParamError)
		reply.Msg = fmt.Sprintf("%s. missing[%s]", reply.Msg, paramName)
		sendReply(w, reply)
		return "", pkg.ErrParamError
	}
	return paramVal, nil
}

func checkMissingAndUint32Param(w http.ResponseWriter, r *http.Request, paramName string) (uint32, error) {
	paramValStr, err := checkMissingParam(w, r, paramName)
	if err != nil {
		return 0, err
	}

	paramValInt, err := strconv.Atoi(paramValStr)
	if err != nil {
		reply := newHttpErrReply(pkg.ErrParamError)
		newMsg := fmt.Sprintf("%s, unmatched type[%s]", reply.Msg, paramName)
		reply.Msg = newMsg
		sendReply(w, reply)
		return 0, pkg.ErrParamError
	}
	if paramValInt > math.MaxUint32 {
		reply := newHttpErrReply(pkg.ErrParamError)
		newMsg := fmt.Sprintf("%s, value of [%s] exceed uint32 limit", reply.Msg, paramName)
		reply.Msg = newMsg
		sendReply(w, reply)
		return 0, pkg.ErrParamError
	}
	return uint32(paramValInt), nil
}

func checkMissingAndUint64Param(w http.ResponseWriter, r *http.Request, paramName string) (uint64, error) {
	paramValStr, err := checkMissingParam(w, r, paramName)
	if err != nil {
		return 0, err
	}

	paramValInt, err := strconv.Atoi(paramValStr)
	if err != nil {
		reply := newHttpErrReply(pkg.ErrParamError)
		newMsg := fmt.Sprintf("%s, unmatched type[%s]", reply.Msg, paramName)
		reply.Msg = newMsg
		sendReply(w, reply)
		return 0, pkg.ErrParamError
	}
	return uint64(paramValInt), nil
}

func checkMissingAndBoolParam(w http.ResponseWriter, r *http.Request, paramName string) (value bool, miss bool) {
	paramVal := r.FormValue(paramName)
	if paramVal == "" {
		return false, true
	}
	value, err := strconv.ParseBool(paramVal)
	if err != nil {
		reply := newHttpErrReply(pkg.ErrParamError)
		newMsg := fmt.Sprintf("%s, unmatched type[%s]", reply.Msg, paramName)
		reply.Msg = newMsg
		sendReply(w, reply)
		return false, true
	}
	return value, false
}

func sendReply(w http.ResponseWriter, httpReply *netutil.HttpReply) {
	log.Debug("response:[%v]", httpReply)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.Error("fail to marshal http reply[%v]. err:[%v]", httpReply, err)
		sendReply(w, newHttpErrReply(pkg.ErrInternalError))
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.Error("fail to write http reply[%s] len[%d]. err:[%v]", string(reply), len(reply), err)
	}
}
