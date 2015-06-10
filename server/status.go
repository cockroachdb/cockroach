// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Shawn Morel (shawn@strangemonad.com)
// Author: Bram Gruneir (bram@cockroachlabs.com)

package server

import (
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/server/status"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/julienschmidt/httprouter"
)

const (
	// stackTraceApproxSize is the approximate size of a goroutine stack trace.
	stackTraceApproxSize = 1024

	// statusKeyPrefix is the root of the RESTful cluster statistics and metrics API.
	statusKeyPrefix = "/_status/"

	// statusGossipKeyPrefix exposes a view of the gossip network.
	statusGossipKeyPrefix = statusKeyPrefix + "gossip"

	// statusLocalKeyPrefix is the key prefix for all local status
	// info. Unadorned, the URL exposes the status of the node serving
	// the request.  This is equivalent to GETing
	// statusNodesKeyPrefix/<current-node-id>.  Useful for debugging
	// nodes that aren't communicating with the cluster properly.
	statusLocalKeyPrefix = statusKeyPrefix + "local/"

	// statusLocalLogKeyPrefix exposes a list of log files for the node.
	// logfiles -> lists available log files
	// logfiles/ -> lists available log files
	// logfiles/{file} -> fetches contents of named log
	statusLocalLogFileKeyPrefix = statusLocalKeyPrefix + "logfiles/"
	// statusLocalLogFileKeyPattern is the pattern to match
	// logfiles/{file}
	statusLocalLogFileKeyPattern = statusLocalLogFileKeyPrefix + ":file"

	// statusLocalLogKeyPrefix exposes a list of logs for the node.
	// log -> equivalent to logs/info
	// log/ -> equivalent to logs/info
	// log/{level} -> returns the logs for the provided level and higher
	statusLocalLogKeyPrefix = statusLocalKeyPrefix + "log/"
	// statusLocalLogKeyPattern is the pattern to match
	// log/{level}
	statusLocalLogKeyPattern = statusLocalLogKeyPrefix + ":level"

	// statusLocalStacksKey exposes stack traces of running goroutines.
	statusLocalStacksKey = statusLocalKeyPrefix + "stacks"

	// statusNodeKeyPrefix exposes status for each of the nodes the cluster.
	// nodes -> lists all nodes
	// nodes/ -> lists all nodes
	// nodes/{NodeID} -> shows only the status for that specific node
	statusNodeKeyPrefix = statusKeyPrefix + "nodes/"
	// statusNodeKeyPattern is the pattern to match
	// nodes/{NodeID}
	statusNodeKeyPattern = statusNodeKeyPrefix + ":id"

	// statusStoreKeyPrefix exposes status for each store.
	// stores -> lists all nodes
	// stores/ -> lists all nodes
	// stores/{StoreID} -> shows only the status for that specific store
	statusStoreKeyPrefix = statusKeyPrefix + "stores/"
	// statusStoreKeyPattern is the pattern to match
	// stores/{StoreID}
	statusStoreKeyPattern = statusStoreKeyPrefix + ":id"

	// statusTransactionsKeyPrefix exposes transaction statistics.
	statusTransactionsKeyPrefix = statusKeyPrefix + "txns/"

	// Default Maximum number of log entries returned.
	defaultMaxLogEntries = 1000
)

// A statusServer provides a RESTful status API.
type statusServer struct {
	db     *client.DB
	gossip *gossip.Gossip
	router *httprouter.Router
}

// newStatusServer allocates and returns a statusServer.
func newStatusServer(db *client.DB, gossip *gossip.Gossip) *statusServer {
	server := &statusServer{
		db:     db,
		gossip: gossip,
		router: httprouter.New(),
	}

	server.router.GET(statusKeyPrefix, server.handleClusterStatus)
	server.router.GET(statusGossipKeyPrefix, server.handleGossipStatus)
	server.router.GET(statusLocalKeyPrefix, server.handleLocalStatus)
	server.router.GET(statusLocalLogFileKeyPrefix, server.handleLocalLogFiles)
	server.router.GET(statusLocalLogFileKeyPattern, server.handleLocalLogFile)
	server.router.GET(statusLocalLogKeyPrefix, server.handleLocalLog)
	server.router.GET(statusLocalLogKeyPattern, server.handleLocalLog)
	server.router.GET(statusLocalStacksKey, server.handleLocalStacks)
	server.router.GET(statusNodeKeyPrefix, server.handleNodesStatus)
	server.router.GET(statusNodeKeyPattern, server.handleNodeStatus)
	server.router.GET(statusStoreKeyPrefix, server.handleStoresStatus)
	server.router.GET(statusStoreKeyPattern, server.handleStoreStatus)
	server.router.GET(statusTransactionsKeyPrefix, server.handleTransactionStatus)

	return server
}

// ServeHTTP implements the http.Handler interface.
func (s *statusServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// handleStatus handles GET requests for cluster status.
func (s *statusServer) handleClusterStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	cluster := &status.Cluster{}
	b, contentType, err := util.MarshalResponse(r, cluster, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleGossipStatus handles GET requests for gossip network status.
func (s *statusServer) handleGossipStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	b, err := s.gossip.GetInfosAsJSON()
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
	}
	w.Write(b)
}

// handleLocalStatus handles GET requests for local-node status.
func (s *statusServer) handleLocalStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	local := struct {
		Address   util.UnresolvedAddr `json:"address"`
		BuildInfo util.BuildInfo      `json:"buildInfo"`
	}{
		BuildInfo: util.GetBuildInfo(),
	}
	if addr, err := s.gossip.GetNodeIDAddress(s.gossip.GetNodeID()); err == nil {
		local.Address = addr.(util.UnresolvedAddr)
	}
	b, contentType, err := util.MarshalResponse(r, local, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleLocalLogFiles handles GET requests for list of available logs.
func (s *statusServer) handleLocalLogFiles(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	log.Flush()
	logFiles, err := log.ListLogFiles()
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	b, contentType, err := util.MarshalResponse(r, logFiles, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleLocalLogFile handles GET requests for a single log. If no filename is
// available, it returns 404. The log contents are returned in structured
// format as JSON.
func (s *statusServer) handleLocalLogFile(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Flush()
	file := ps.ByName("file")
	reader, err := log.GetLogReader(file, false /* !allowAbsolute */)
	if reader == nil || err != nil {
		log.Errorf("unable to open log file %s: %s", file, err)
		http.NotFound(w, r)
		return
	}
	defer reader.Close()

	entry := proto.LogEntry{}
	var entries []proto.LogEntry
	decoder := log.NewEntryDecoder(reader)
	for {
		if err := decoder.Decode(&entry); err != nil {
			if err == io.EOF {
				break
			}
			log.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		entries = append(entries, entry)
	}

	b, contentType, err := util.MarshalResponse(r, entries, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// parseInt64WithDefault attempts to parse the passed in string. If an empty
// string is supplied or parsing results in an error the default value is
// returned.  If an error does occur during parsing, the error is returned as
// well.
func parseInt64WithDefault(s string, defaultValue int64) (int64, error) {
	if len(s) == 0 {
		return defaultValue, nil
	}
	result, err := strconv.ParseInt(s, 10, 0)
	if err != nil {
		return defaultValue, err
	}
	return result, nil
}

// handleLocalLog returns the log entries parsed from the log file that occurred
// after or on the query parameter "starttime" and before or on the query
// parameter "endtime". Additionally, if a level is specified via the URL, the
// log entries are scoped to that level. If no "starttime" is provided, then
// a "starttime" of a day ago is used.  If no "endtime" is provided, the current
// time is used. The cutoff for entries is defaults to defaultMaxLogEntries but
// can be adjusted via the "max" query parameter.
func (s *statusServer) handleLocalLog(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	log.Flush()
	level := ps.ByName("level")
	var sev log.Severity
	if len(level) == 0 {
		sev = log.InfoLog
	} else {
		var sevFound bool
		sev, sevFound = log.SeverityByName(level)
		if !sevFound {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "could not determine level %s", level)
			return
		}
	}

	startTimestamp, err := parseInt64WithDefault(
		r.URL.Query().Get("starttime"),
		time.Now().AddDate(0, 0, -1).UnixNano())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "starttime could not be parsed:%s", err)
		return
	}

	endTimestamp, err := parseInt64WithDefault(
		r.URL.Query().Get("endtime"),
		time.Now().UnixNano())
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "endtime could not be parsed:%s", err)
		return
	}

	if startTimestamp > endTimestamp {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "startime:%d is greater than endtime:%d", startTimestamp, endTimestamp)
		return
	}

	maxEntries, err := parseInt64WithDefault(
		r.URL.Query().Get("max"),
		defaultMaxLogEntries)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "max could not be parsed:%s", err)
		return
	}
	if maxEntries < 1 {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "max must be set to a value greater than 0")
		return
	}

	entries, err := log.FetchEntriesFromFiles(sev, startTimestamp, endTimestamp, int(maxEntries))
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	b, contentType, err := util.MarshalResponse(r, entries, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleLocalStacks handles GET requests for goroutines stack traces.
func (s *statusServer) handleLocalStacks(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	bufSize := runtime.NumGoroutine() * stackTraceApproxSize
	for {
		buf := make([]byte, bufSize)
		length := runtime.Stack(buf, true)
		// If this wasn't large enough to accommodate the full set of
		// stack traces, increase by 2 and try again.
		if length == bufSize {
			bufSize = bufSize * 2
			continue
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Write(buf[:length])
		return
	}
}

// handleNodesStatus handles GET requests for all node statuses.
func (s *statusServer) handleNodesStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	startKey := keys.StatusNodePrefix
	endKey := startKey.PrefixEnd()

	rows, err := s.db.Scan(startKey, endKey, 0)
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	nodeStatuses := []proto.NodeStatus{}
	for _, row := range rows {
		nodeStatus := &proto.NodeStatus{}
		if err := row.ValueProto(nodeStatus); err != nil {
			log.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		nodeStatuses = append(nodeStatuses, *nodeStatus)
	}
	b, contentType, err := util.MarshalResponse(r, nodeStatuses, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleNodeStatus handles GET requests for a single node's status. If no id is
// available, it calls handleNodesStatus to return all node's statuses.
func (s *statusServer) handleNodeStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id, err := strconv.ParseInt(ps.ByName("id"), 10, 64)
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	key := keys.NodeStatusKey(int32(id))

	nodeStatus := &proto.NodeStatus{}
	if err := s.db.GetProto(key, nodeStatus); err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	b, contentType, err := util.MarshalResponse(r, nodeStatus, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleStoresStatus handles GET requests for all store statuses.
func (s *statusServer) handleStoresStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	startKey := keys.StatusStorePrefix
	endKey := startKey.PrefixEnd()

	rows, err := s.db.Scan(startKey, endKey, 0)
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	storeStatuses := []proto.StoreStatus{}
	for _, row := range rows {
		storeStatus := &proto.StoreStatus{}
		if err := row.ValueProto(storeStatus); err != nil {
			log.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		storeStatuses = append(storeStatuses, *storeStatus)
	}
	b, contentType, err := util.MarshalResponse(r, storeStatuses, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleStoreStatus handles GET requests for a single node's status. If no id
// is available, it calls handleStoresStatus to return all store's statuses.
func (s *statusServer) handleStoreStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id, err := strconv.ParseInt(ps.ByName("id"), 10, 32)
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	key := keys.StoreStatusKey(int32(id))

	storeStatus := &proto.StoreStatus{}
	if err := s.db.GetProto(key, storeStatus); err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	b, contentType, err := util.MarshalResponse(r, storeStatus, []util.EncodingType{util.JSONEncoding})
	if err != nil {
		log.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Write(b)
}

// handleTransactionStatus handles GET requests for transaction status.
func (s *statusServer) handleTransactionStatus(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"transactions": []}`))
}
