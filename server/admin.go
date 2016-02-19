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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Bram Gruneir (bram+code@cockroachlabs.com)
// Author: Cuong Do (cdo@cockroachlabs.com)

package server

import (
	// This is imported for its side-effect of registering expvar
	// endpoints with the http.DefaultServeMux.
	_ "expvar"
	"fmt"
	"net/http"
	"strings"
	"time"

	// Register the net/trace endpoint with http.DefaultServeMux.
	"golang.org/x/net/trace"
	// This is imported for its side-effect of registering pprof
	// endpoints with the http.DefaultServeMux.
	_ "net/http/pprof"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"

	"github.com/julienschmidt/httprouter"
)

func init() {
	// Tweak the authentication logic for the tracing endpoint.
	// By default it's open for localhost only, but with Docker
	// we want to get there from anywhere.
	// TODO(mberhault): properly secure this once we require client certs.
	trace.AuthRequest = func(_ *http.Request) (bool, bool) {
		// Open-door policy except traces marked "sensitive".
		return true, false
	}
}

const (
	// adminEndpoint is the prefix for RESTful endpoints used to
	// provide an administrative interface to the cockroach cluster.
	adminEndpoint = "/_admin/"
	// debugEndpoint is the prefix of golang's standard debug functionality
	// for access to exported vars and pprof tools.
	debugEndpoint = "/debug/"
	// healthPath is the health endpoint.
	healthPath = adminEndpoint + "health"
	// quitPath is the quit endpoint.
	quitPath = adminEndpoint + "quit"
	// apiEndpoint is the prefix for the RESTful API used by the admin UI.
	apiEndpoint = adminEndpoint + "api/v1/"
	// databasesPath is the endpoint for listing databases.
	databasesPath = apiEndpoint + "databases"
	// databasesPath is the endpoint for listing databases.
	databaseDetailsPattern = databasesPath + "/:database"
)

// An actionHandler is an interface which provides Get, Put & Delete
// to satisfy administrative REST APIs.
type actionHandler interface {
	Put(path string, body []byte, r *http.Request) error
	Get(path string, r *http.Request) (body []byte, contentType string, err error)
	Delete(path string, r *http.Request) error
}

// A adminServer provides a RESTful HTTP API to administration of
// the cockroach cluster.
type adminServer struct {
	db          *client.DB    // Key-value database client
	stopper     *stop.Stopper // Used to shutdown the server
	sqlExecutor *sql.Executor
	router      *httprouter.Router
}

// newAdminServer allocates and returns a new REST server for
// administrative APIs.
func newAdminServer(db *client.DB, stopper *stop.Stopper, sqlExecutor *sql.Executor) *adminServer {
	server := &adminServer{
		db:          db,
		stopper:     stopper,
		sqlExecutor: sqlExecutor,
		router:      httprouter.New(),
	}

	server.router.GET(debugEndpoint+"*path", server.handleDebug)
	server.router.GET(healthPath, server.handleHealth)
	server.router.GET(quitPath, server.handleQuit)
	server.router.GET(databasesPath, server.handleDatabases)
	server.router.GET(databaseDetailsPattern, server.handleDatabaseDetails)
	return server
}

// ServeHTTP implements http.Handler.
func (s *adminServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// handleHealth responds to health requests from monitoring services.
func (s *adminServer) handleHealth(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set(util.ContentTypeHeader, util.PlaintextContentType)
	fmt.Fprintln(w, "ok")
}

// handleQuit is the shutdown hook. The server is first placed into a
// draining mode, followed by exit.
func (s *adminServer) handleQuit(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set(util.ContentTypeHeader, util.PlaintextContentType)
	fmt.Fprintln(w, "ok")
	go func() {
		time.Sleep(50 * time.Millisecond)
		s.stopper.Stop()
	}()
}

// handleDebug passes requests with the debugPathPrefix onto the default
// serve mux, which is preconfigured (by import of expvar and net/http/pprof)
// to serve endpoints which access exported variables and pprof tools.
func (s *adminServer) handleDebug(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	handler, _ := http.DefaultServeMux.Handler(r)
	handler.ServeHTTP(w, r)
}

// handleDatabases is an endpoint that responds with a JSON object listing all databases.
func (s *adminServer) handleDatabases(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var session sql.Session
	resp, _, err := s.sqlExecutor.ExecuteStatements("root", session, "SHOW DATABASES", nil)
	if err != nil {
		log.Errorf("handleDatabases: %s", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, res := range resp.Results {
		if res.PErr != nil {
			log.Error(res.PErr)
			http.Error(w, res.PErr.String(), http.StatusInternalServerError)
			return
		}
	}

	// It would be natural to just return a slice as the JSON response object. However, when
	// marshalling JSON, we always enclose top-level slices in an outer object to work around this
	// vulnerability: http://haacked.com/archive/2009/06/25/json-hijacking.aspx/
	//
	// So, it seems cleaner to wrap the results in a more obvious way.
	databases := sqlResultToSlice(resp.Results[0])
	result := map[string]interface{}{
		"Databases": databases,
	}
	respondAsJSON(w, r, result)
}

// extractDatabase extracts the ":database" parameter from URLs.
func (s *adminServer) extractDatabase(ps httprouter.Params) (string, error) {
	databaseParam := ps.ByName("database")
	if len(databaseParam) == 0 {
		return "", util.Errorf("no database parameter provided")
	}
	return databaseParam, nil
}

// sqlResultToSlice returns a slice containing the value of the first column of each row in the
// provided SQL result. This useful for results containing a single column.
func sqlResultToSlice(result sql.Result) []interface{} {
	var rows []interface{}
	for _, r := range result.Rows {
		rows = append(rows, r.Values[0])
	}
	return rows
}

// sqlResultToMaps returns a list of maps of column name -> column value for all rows in the
// given SQL query result.
func sqlResultToMaps(result sql.Result) []map[string]interface{} {
	var rows []map[string]interface{}
	for _, r := range result.Rows {
		if len(result.Columns) == 1 {
		}
		row := make(map[string]interface{})
		for i, col := range result.Columns {
			row[col.Name] = r.Values[i]
		}
		rows = append(rows, row)
	}

	return rows
}

// handleDatabase is an endpoint that returns grants and a list of tables for the specified
// database.
func (s *adminServer) handleDatabaseDetails(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	var session sql.Session
	dbname, err := s.extractDatabase(ps)

	query := fmt.Sprintf("SHOW GRANTS ON DATABASE %s; SHOW TABLES FROM %s;", dbname, dbname)
	resp, _, err := s.sqlExecutor.ExecuteStatements("root", session, query, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	for _, res := range resp.Results {
		if res.PErr != nil {
			if strings.HasSuffix(res.PErr.String(), "does not exist") {
				http.Error(w, res.PErr.String(), http.StatusNotFound)
				return
			}

			log.Error(res.PErr)
			http.Error(w, res.PErr.String(), http.StatusInternalServerError)
			return
		}
	}

	// Put the results of the queries in JSON-friendly objects. For grants, we split the comma-
	// separated lists of privileges into proper slices.
	const privilegesKey = "Privileges"
	grants := sqlResultToMaps(resp.Results[0])
	for _, grant := range grants {
		privileges := string(grant[privilegesKey].(parser.DString))
		grant[privilegesKey] = strings.Split(privileges, ",")
	}
	tables := sqlResultToSlice(resp.Results[1])
	result := map[string]interface{}{
		"Grants": grants,
		"Tables": tables,
	}
	respondAsJSON(w, r, result)
}
