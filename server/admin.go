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
	"bytes"
	"errors"
	// This is imported for its side-effect of registering expvar
	// endpoints with the http.DefaultServeMux.
	_ "expvar"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"
	// Register the net/trace endpoint with http.DefaultServeMux.
	"golang.org/x/net/trace"
	// This is imported for its side-effect of registering pprof
	// endpoints with the http.DefaultServeMux.
	_ "net/http/pprof"

	gwruntime "github.com/gengo/grpc-gateway/runtime"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/kv"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

func init() {
	// Tweak the authentication logic for the tracing endpoint. By default it's
	// open for localhost only, but with Docker we want to get there from
	// anywhere. We maintain the default behavior of only allowing access to
	// sensitive logs from localhost.
	//
	// TODO(mberhault): properly secure this once we require client certs.
	origAuthRequest := trace.AuthRequest
	trace.AuthRequest = func(req *http.Request) (bool, bool) {
		_, sensitive := origAuthRequest(req)
		return true, sensitive
	}
}

const (
	// debugEndpoint is the prefix of golang's standard debug functionality
	// for access to exported vars and pprof tools.
	debugEndpoint = "/debug/"

	// adminEndpoint is the prefix for RESTful endpoints used to
	// provide an administrative interface to the cockroach cluster.
	adminEndpoint = "/_admin/"
	// apiEndpoint is the prefix for the RESTful API used by the admin UI.
	apiEndpoint = adminEndpoint + "v1/"
	// healthPath is the health endpoint.
	healthPath = apiEndpoint + "health"
	// quitPath is the quit endpoint.
	quitPath = apiEndpoint + "quit"

	// eventLimit is the maximum number of events returned by any endpoints
	// returning events.
	apiEventLimit = 1000
)

var (
	errUIKeyNotFound = errors.New("key not found")

	// apiServerMessage is the standard body for all HTTP 500 responses.
	errAdminAPIError = grpc.Errorf(codes.Internal, "An internal server error has occurred. Please "+
		"check your CockroachDB logs for more details.")
)

// A adminServer provides a RESTful HTTP API to administration of
// the cockroach cluster.
type adminServer struct {
	db          *client.DB    // Key-value database client
	stopper     *stop.Stopper // Used to shutdown the server
	sqlExecutor *sql.Executor
	*http.ServeMux
	distSender *kv.DistSender

	// Mux provided by grpc-gateway to handle HTTP/gRPC proxying.
	gwMux *gwruntime.ServeMux

	// Context for grpc-gateway.
	gwCtx context.Context

	// Cancels outstanding grpc-gateway operations.
	gwCancel context.CancelFunc
}

// newAdminServer allocates and returns a new REST server for
// administrative APIs.
func newAdminServer(db *client.DB, stopper *stop.Stopper, sqlExecutor *sql.Executor, ds *kv.DistSender) *adminServer {
	server := &adminServer{
		db:          db,
		stopper:     stopper,
		sqlExecutor: sqlExecutor,
		ServeMux:    http.NewServeMux(),
		distSender:  ds,
	}

	// Register HTTP handlers.
	server.ServeMux.HandleFunc(debugEndpoint, server.handleDebug)
	// TODO(cdo): Move quit and health endpoints to gRPC.
	server.ServeMux.HandleFunc(quitPath, server.handleQuit)
	server.ServeMux.HandleFunc(healthPath, server.handleHealth)

	// Initialize grpc-gateway mux and context.
	server.gwMux = gwruntime.NewServeMux()
	server.gwCtx, server.gwCancel = context.WithCancel(context.Background())

	return server
}

// RegisterGRPCGateway starts the gateway (i.e. reverse proxy) that proxies
// HTTP requests to the appropriate gRPC endpoints.
func (s *adminServer) RegisterGRPCGateway(serverCtx *Context) error {
	// Setup HTTP<->gRPC handlers.
	var opts []grpc.DialOption
	if serverCtx.Insecure {
		opts = append(opts, grpc.WithInsecure())
	} else {
		tlsConfig, err := serverCtx.GetClientTLSConfig()
		if err != nil {
			return err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}
	err := RegisterAdminHandlerFromEndpoint(s.gwCtx, s.gwMux, serverCtx.Addr, opts)
	if err != nil {
		return err
	}

	// Pass all requests for gRPC-based API endpoints to the gateway mux.
	s.ServeMux.Handle(apiEndpoint, s.gwMux)
	return nil
}

// Close cleans up resources used by the adminServer.
func (s *adminServer) Close() {
	s.gwCancel()
}

// handleHealth responds to health requests from monitoring services.
func (s *adminServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(util.ContentTypeHeader, util.PlaintextContentType)
	fmt.Fprintln(w, "ok")
}

// handleQuit is the shutdown hook. The server is first placed into a
// draining mode, followed by exit.
func (s *adminServer) handleQuit(w http.ResponseWriter, r *http.Request) {
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
func (s *adminServer) handleDebug(w http.ResponseWriter, r *http.Request) {
	handler, _ := http.DefaultServeMux.Handler(r)
	handler.ServeHTTP(w, r)
}

// getUserProto will return the authenticated user. For now, this is just a stub until we
// figure out our authentication mechanism.
//
// TODO(cdo): Make this work when we have an authentication scheme for the
// API.
func (s *adminServer) getUser(_ proto.Message) string {
	return security.RootUser
}

// serverError logs the provided error and returns an error that should be returned by
// the RPC endpoint method.
func (s *adminServer) serverError(err error) error {
	log.ErrorfDepth(1, "%s", err)
	return errAdminAPIError
}

// serverErrorf logs the provided error and returns an error that should be returned by
// the RPC endpoint method.
func (s *adminServer) serverErrorf(format string, args ...interface{}) error {
	log.ErrorfDepth(1, format, args...)
	return errAdminAPIError
}

// serverErrors logs the provided errors and returns an error that should be returned by
// the RPC endpoint method.
func (s *adminServer) serverErrors(errors []error) error {
	log.ErrorfDepth(1, "%v", errors)
	return errAdminAPIError
}

// checkQueryResults performs basic tests on the provided query results and returns
// the first error that was found.
func (s *adminServer) checkQueryResults(results []sql.Result, numResults int) error {
	if a, e := len(results), numResults; a != e {
		return util.Errorf("# of results %d != expected %d", a, e)
	}

	for _, result := range results {
		if result.PErr != nil {
			return util.Errorf("%s", result.PErr.String())
		}
	}

	return nil
}

// firstNotFoundError returns the first table/database not found error in the
// provided results.
func (s *adminServer) firstNotFoundError(results []sql.Result) *roachpb.Error {
	for _, res := range results {
		// TODO(cdo): Replace this crude suffix-matching with something more structured once we have
		// more structured errors.
		if pErr := res.PErr; pErr != nil && strings.HasSuffix(pErr.String(), "does not exist") {
			return pErr
		}
	}

	return nil
}

// Databases is an endpoint that returns a list of databases.
func (s *adminServer) Databases(ctx context.Context, req *DatabasesRequest) (*DatabasesResponse, error) {
	var session sql.Session
	user := s.getUser(req)
	r := s.sqlExecutor.ExecuteStatements(ctx, user, &session, "SHOW DATABASES;", nil)
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return nil, s.serverError(err)
	}

	var resp DatabasesResponse
	for _, row := range r.ResultList[0].Rows {
		dbname, ok := row.Values[0].(parser.DString)
		if !ok {
			return nil, s.serverErrorf("type assertion failed on db name: %T", row.Values[0])
		}
		resp.Databases = append(resp.Databases, string(dbname))
	}

	return &resp, nil
}

// DatabaseDetails is an endpoint that returns grants and a list of table names
// for the specified database.
func (s *adminServer) DatabaseDetails(ctx context.Context, req *DatabaseDetailsRequest) (*DatabaseDetailsResponse, error) {
	var session sql.Session
	user := s.getUser(req)

	// Placeholders don't work with SHOW statements, so we need to manually
	// escape the database name.
	//
	// TODO(cdo): Use placeholders when they're supported by SHOW.
	escDBName := parser.Name(req.Database).String()
	query := fmt.Sprintf("SHOW GRANTS ON DATABASE %s; SHOW TABLES FROM %s;", escDBName, escDBName)
	r := s.sqlExecutor.ExecuteStatements(ctx, user, &session, query, nil)
	if pErr := s.firstNotFoundError(r.ResultList); pErr != nil {
		return nil, grpc.Errorf(codes.NotFound, "%s", pErr)
	}
	if err := s.checkQueryResults(r.ResultList, 2); err != nil {
		return nil, s.serverError(err)
	}

	// Marshal grants.
	var resp DatabaseDetailsResponse
	{
		const (
			userCol       = "User"
			privilegesCol = "Privileges"
		)

		scanner := newResultScanner(r.ResultList[0].Columns)
		for _, row := range r.ResultList[0].Rows {
			// Marshal grant, splitting comma-separated privileges into a proper slice.
			var grant DatabaseDetailsResponse_Grant
			var privileges string
			if err := scanner.Scan(row, userCol, &grant.User); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, privilegesCol, &privileges); err != nil {
				return nil, err
			}
			grant.Privileges = strings.Split(privileges, ",")
			resp.Grants = append(resp.Grants, &grant)
		}
	}

	// Marshal table names.
	{
		const tableCol = "Table"
		scanner := newResultScanner(r.ResultList[1].Columns)
		if a, e := len(r.ResultList[1].Columns), 1; a != e {
			return nil, s.serverErrorf("show tables columns mismatch: %d != expected %d", a, e)
		}
		for _, row := range r.ResultList[1].Rows {
			var tableName string
			if err := scanner.Scan(row, tableCol, &tableName); err != nil {
				return nil, err
			}
			resp.TableNames = append(resp.TableNames, tableName)
		}
	}

	return &resp, nil
}

// TableDetails is an endpoint that returns columns, indices, and other
// relevant details for the specified table.
func (s *adminServer) TableDetails(ctx context.Context, req *TableDetailsRequest) (
	*TableDetailsResponse, error) {
	var session sql.Session
	user := s.getUser(req)

	// TODO(cdo): Use real placeholders for the table and database names when we've extended our SQL
	// grammar to allow that.
	escDbName := parser.Name(req.Database).String()
	escTableName := parser.Name(req.Table).String()
	escQualTable := fmt.Sprintf("%s.%s", escDbName, escTableName)
	query := fmt.Sprintf("SHOW COLUMNS FROM %s; SHOW INDEX FROM %s; SHOW GRANTS ON TABLE %s",
		escQualTable, escQualTable, escQualTable)
	r := s.sqlExecutor.ExecuteStatements(ctx, user, &session, query, nil)
	if pErr := s.firstNotFoundError(r.ResultList); pErr != nil {
		return nil, grpc.Errorf(codes.NotFound, "%s", pErr)
	}
	if err := s.checkQueryResults(r.ResultList, 3); err != nil {
		return nil, err
	}

	var resp TableDetailsResponse

	// Marshal SHOW COLUMNS result.
	//
	// TODO(cdo): protobuf v3's default behavior for fields with zero values (e.g. empty strings)
	// is to suppress them. So, if protobuf field "foo" is an empty string, "foo" won't show
	// up in the marshalled JSON. I feel that this is counterintuitive, and this should be fixed
	// for our API.
	{
		const (
			fieldCol   = "Field" // column name
			typeCol    = "Type"
			nullCol    = "Null"
			defaultCol = "Default"
		)
		scanner := newResultScanner(r.ResultList[0].Columns)
		for _, row := range r.ResultList[0].Rows {
			var col TableDetailsResponse_Column
			if err := scanner.Scan(row, fieldCol, &col.Name); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, typeCol, &col.Type); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, nullCol, &col.Nullable); err != nil {
				return nil, err
			}
			isDefaultNull, err := scanner.IsNull(row, defaultCol)
			if err != nil {
				return nil, err
			}
			if !isDefaultNull {
				if err := scanner.Scan(row, defaultCol, &col.Default); err != nil {
					return nil, err
				}
			}
			resp.Columns = append(resp.Columns, &col)
		}
	}

	// Marshal SHOW INDEX result.
	{
		const (
			nameCol      = "Name"
			uniqueCol    = "Unique"
			seqCol       = "Seq"
			columnCol    = "Column"
			directionCol = "Direction"
			storingCol   = "Storing"
		)
		scanner := newResultScanner(r.ResultList[1].Columns)
		for _, row := range r.ResultList[1].Rows {
			// Marshal grant, splitting comma-separated privileges into a proper slice.
			var index TableDetailsResponse_Index
			if err := scanner.Scan(row, nameCol, &index.Name); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, uniqueCol, &index.Unique); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, seqCol, &index.Seq); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, columnCol, &index.Column); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, directionCol, &index.Direction); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, storingCol, &index.Storing); err != nil {
				return nil, err
			}
			resp.Indexes = append(resp.Indexes, &index)
		}
	}

	// Marshal SHOW GRANTS result.
	{
		const (
			userCol       = "User"
			privilegesCol = "Privileges"
		)
		scanner := newResultScanner(r.ResultList[2].Columns)
		for _, row := range r.ResultList[2].Rows {
			// Marshal grant, splitting comma-separated privileges into a proper slice.
			var grant TableDetailsResponse_Grant
			var privileges string
			if err := scanner.Scan(row, userCol, &grant.User); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, privilegesCol, &privileges); err != nil {
				return nil, err
			}
			grant.Privileges = strings.Split(privileges, ",")
			resp.Grants = append(resp.Grants, &grant)
		}
	}

	// Get the number of ranges in the table. We get the key span for the table
	// data. Then, we count the number of ranges that make up that key span.
	{
		var iexecutor sql.InternalExecutor
		var tableSpan roachpb.Span
		if pErr := s.db.Txn(ctx, func(txn *client.Txn) *roachpb.Error {
			var pErr *roachpb.Error
			tableSpan, pErr = iexecutor.GetTableSpan(user, txn, escDbName, escTableName)
			return pErr
		}); pErr != nil {
			return nil, s.serverError(pErr.GoError())
		}
		tableRSpan := roachpb.RSpan{
			Key:    keys.Addr(tableSpan.Key),
			EndKey: keys.Addr(tableSpan.EndKey),
		}
		rangeCount, pErr := s.distSender.CountRanges(tableRSpan)
		if pErr != nil {
			return nil, s.serverError(pErr.GoError())
		}
		resp.RangeCount = rangeCount
	}

	return &resp, nil
}

// Users returns a list of users, stripped of any passwords.
func (s *adminServer) Users(ctx context.Context, req *UsersRequest) (*UsersResponse, error) {
	var session sql.Session
	user := s.getUser(req)
	query := "SELECT username FROM system.users"
	r := s.sqlExecutor.ExecuteStatements(ctx, user, &session, query, nil)
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return nil, s.serverError(err)
	}

	var resp UsersResponse
	for _, row := range r.ResultList[0].Rows {
		resp.Users = append(resp.Users, &UsersResponse_User{string(row.Values[0].(parser.DString))})
	}
	return &resp, nil
}

// Events is an endpoint that returns the latest event log entries, with the following
// optional URL parameters:
//
// type=STRING  returns events with this type (e.g. "create_table")
// targetID=INT returns events for that have this targetID
func (s *adminServer) Events(ctx context.Context, req *EventsRequest) (*EventsResponse, error) {
	var session sql.Session
	user := s.getUser(req)

	// Execute the query.
	q := &sqlQuery{}
	q.Append("SELECT timestamp, eventType, targetID, reportingID, info, uniqueID ")
	q.Append("FROM system.eventlog ")
	q.Append("WHERE true ") // This simplifies the WHERE clause logic below.
	if len(req.Type) > 0 {
		q.Append("AND eventType = $ ", parser.DString(req.Type))
	}
	if req.TargetId > 0 {
		q.Append("AND targetID = $ ", parser.DInt(req.TargetId))
	}
	q.Append("ORDER BY timestamp DESC ")
	q.Append("LIMIT $", parser.DInt(apiEventLimit))
	if len(q.Errors()) > 0 {
		return nil, s.serverErrors(q.Errors())
	}
	r := s.sqlExecutor.ExecuteStatements(ctx, user, &session, q.String(), q.Params())
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return nil, s.serverError(err)
	}

	// Marshal response.
	var resp EventsResponse
	scanner := newResultScanner(r.ResultList[0].Columns)
	for _, row := range r.ResultList[0].Rows {
		var event EventsResponse_Event
		var ts time.Time
		if err := scanner.ScanIndex(row, 0, &ts); err != nil {
			return nil, err
		}
		nanos := ts.UnixNano()
		event.Timestamp = &EventsResponse_Event_Timestamp{Sec: nanos / 1e9, Nsec: uint32(nanos % 1e9)}
		if err := scanner.ScanIndex(row, 1, &event.EventType); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 2, &event.TargetID); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 3, &event.ReportingID); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 4, &event.Info); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 5, &event.UniqueID); err != nil {
			return nil, err
		}

		resp.Events = append(resp.Events, &event)
	}
	return &resp, nil
}

// getUIData returns the value and timestamp for the given UI key. Returns
// errUIKeyNotFound if the key was not found.
func (s *adminServer) getUIData(session *sql.Session, user, key string) ([]byte, GetUIDataResponse_Timestamp, error) {
	ctx := context.TODO()
	zeroTimestamp := GetUIDataResponse_Timestamp{}

	// Query database.
	query := "SELECT value, lastUpdated FROM system.ui WHERE key = $1"
	params := []parser.Datum{parser.DString(key)}
	r := s.sqlExecutor.ExecuteStatements(ctx, user, session, query, params)
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return nil, zeroTimestamp, s.serverError(err)
	}
	if len(r.ResultList[0].Rows) == 0 {
		return nil, zeroTimestamp, errUIKeyNotFound
	}

	// Marshal results.
	row := r.ResultList[0].Rows[0]
	dBytes, ok := row.Values[0].(parser.DBytes)
	if !ok {
		return nil, zeroTimestamp, s.serverErrorf("unexpected type for UI value: %T", row.Values[0])
	}
	dTS, ok := row.Values[1].(parser.DTimestamp)
	if !ok {
		return nil, zeroTimestamp,
			s.serverErrorf("unexpected type for UI lastUpdated: %T", row.Values[1])
	}
	nanos := dTS.UnixNano()
	ts := GetUIDataResponse_Timestamp{nanos / 1e9, uint32(nanos % 1e9)}
	return []byte(dBytes), ts, nil
}

// SetUIData is an endpoint that sets the data associated with a key.
func (s *adminServer) SetUIData(ctx context.Context, req *SetUIDataRequest) (*SetUIDataResponse, error) {
	if len(req.Key) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "key cannot be empty")
	}

	var session sql.Session
	user := s.getUser(req)

	// Do an upsert of the key.
	br := s.sqlExecutor.ExecuteStatements(ctx, user, &session, "BEGIN;", nil)
	if err := s.checkQueryResults(br.ResultList, 1); err != nil {
		return nil, s.serverError(err)
	}

	// See if the key already exists.
	alreadyExists := true
	if _, _, err := s.getUIData(&session, user, req.Key); err != nil {
		if err != errUIKeyNotFound {
			return nil, s.serverError(err)
		}
		alreadyExists = false
	}

	// INSERT or UPDATE as appropriate.
	if alreadyExists {
		query := "UPDATE system.ui SET value = $1, lastUpdated = NOW() WHERE key = $2; COMMIT;"
		params := []parser.Datum{
			parser.DString(req.Value), // $1
			parser.DString(req.Key),   // $2
		}
		r := s.sqlExecutor.ExecuteStatements(ctx, user, &session, query, params)
		if err := s.checkQueryResults(r.ResultList, 2); err != nil {
			return nil, s.serverError(err)
		}
		if a, e := r.ResultList[0].RowsAffected, 1; a != e {
			return nil, s.serverErrorf("rows affected %d != expected %d", a, e)
		}
	} else {
		query := "INSERT INTO system.ui (key, value, lastUpdated) VALUES ($1, $2, NOW()); COMMIT;"
		params := []parser.Datum{
			parser.DString(req.Key),  // $1
			parser.DBytes(req.Value), // $2
		}
		r := s.sqlExecutor.ExecuteStatements(ctx, user, &session, query, params)
		if err := s.checkQueryResults(r.ResultList, 2); err != nil {
			return nil, s.serverError(err)
		}
		if a, e := r.ResultList[0].RowsAffected, 1; a != e {
			return nil, s.serverErrorf("rows affected %d != expected %d", a, e)
		}
	}

	return &SetUIDataResponse{}, nil
}

// GetUIData returns data associated with the given key, which was stored
// earlier through SetUIData.
func (s *adminServer) GetUIData(_ context.Context, req *GetUIDataRequest) (*GetUIDataResponse, error) {
	var session sql.Session
	user := s.getUser(req)

	if len(req.Key) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "key cannot be empty")
	}

	val, ts, err := s.getUIData(&session, user, req.Key)
	if err != nil {
		if err == errUIKeyNotFound {
			return nil, grpc.Errorf(codes.NotFound, "key %s not found", req.Key)
		}
		return nil, s.serverError(err)
	}

	return &GetUIDataResponse{Value: val, LastUpdated: &ts}, nil
}

// sqlQuery allows you to incrementally build a SQL query that uses
// placeholders. Instead of specific placeholders like $1, you instead use the
// temporary placeholder $.
type sqlQuery struct {
	buf    bytes.Buffer
	pidx   int
	params []parser.Datum
	errs   []error
}

// String returns the full query.
func (q *sqlQuery) String() string {
	if len(q.errs) > 0 {
		return "couldn't generate query: please check Errors()"
	}
	return q.buf.String()
}

// Errors returns a slice containing all errors that have happened during the
// construction of this query.
func (q *sqlQuery) Errors() []error {
	return q.errs
}

// Params returns a slice containing all parameters that have been passed into
// this query through Append.
func (q *sqlQuery) Params() []parser.Datum {
	return q.params
}

// Append appends the provided string and any number of query parameters.
// Instead of using normal placeholders (e.g. $1, $2), use meta-placeholder $.
// This method rewrites the query so that it uses proper placeholders.
//
// For example, suppose we have the following calls:
//
//   query.Append("SELECT * FROM foo WHERE a > $ AND a < $ ", arg1, arg2)
//   query.Append("LIMIT $", limit)
//
// The query is rewritten into:
//
//   SELECT * FROM foo WHERE a > $1 AND a < $2 LIMIT $3
//   /* $1 = arg1, $2 = arg2, $3 = limit */
//
// Note that this method does NOT return any errors. Instead, we queue up
// errors, which can later be accessed. Returning an error here would make
// query construction code exceedingly tedious.
func (q *sqlQuery) Append(s string, params ...parser.Datum) {
	var placeholders int
	for _, r := range s {
		q.buf.WriteRune(r)
		if r == '$' {
			q.pidx++
			placeholders++
			q.buf.WriteString(strconv.FormatInt(int64(q.pidx), 10)) // SQL placeholders are 1-based
		}
	}

	if placeholders != len(params) {
		q.errs = append(q.errs,
			util.Errorf("# of placeholders %d != # of params %d", placeholders, len(params)))
	}
	q.params = append(q.params, params...)
}

// resultScanner scans columns from sql.ResultRow instances into variables,
// performing the appropriate casting and error detection along the way.
type resultScanner struct {
	colNameToIdx map[string]int
}

func newResultScanner(cols []sql.ResultColumn) *resultScanner {
	rs := resultScanner{
		colNameToIdx: make(map[string]int),
	}
	for i, col := range cols {
		rs.colNameToIdx[col.Name] = i
	}
	return &rs
}

// IsNull returns whether the specified column of the given row contains
// a SQL NULL value.
func (rs *resultScanner) IsNull(row sql.ResultRow, col string) (bool, error) {
	idx, ok := rs.colNameToIdx[col]
	if !ok {
		return false, util.Errorf("result is missing column %s", col)
	}
	return row.Values[idx] == parser.DNull, nil
}

// ScanIndex scans the given column index of the given row into dst.
func (rs *resultScanner) ScanIndex(row sql.ResultRow, index int, dst interface{}) error {
	src := row.Values[index]

	switch d := dst.(type) {
	case *string:
		if dst == nil {
			return util.ErrorfSkipFrames(1, "nil destination pointer passed in")
		}
		s, ok := src.(parser.DString)
		if !ok {
			return util.ErrorfSkipFrames(1, "source type assertion failed")
		}
		*d = string(s)

	case *bool:
		if dst == nil {
			return util.ErrorfSkipFrames(1, "nil destination pointer passed in")
		}
		s, ok := src.(parser.DBool)
		if !ok {
			return util.ErrorfSkipFrames(1, "source type assertion failed")
		}
		*d = bool(s)

	case *int64:
		if dst == nil {
			return util.ErrorfSkipFrames(1, "nil destination pointer passed in")
		}
		s, ok := src.(parser.DInt)
		if !ok {
			return util.ErrorfSkipFrames(1, "source type assertion failed")
		}
		*d = int64(s)

	case *time.Time:
		if dst == nil {
			return util.ErrorfSkipFrames(1, "nil destination pointer passed in")
		}
		s, ok := src.(parser.DTimestamp)
		if !ok {
			return util.ErrorfSkipFrames(1, "source type assertion failed")
		}
		*d = time.Time(s.Time)

	case *[]byte:
		if dst == nil {
			return util.ErrorfSkipFrames(1, "nil destination pointer passed in")
		}
		s, ok := src.(parser.DBytes)
		if !ok {
			return util.ErrorfSkipFrames(1, "source type assertion failed")
		}
		// Yes, this copies, but this probably isn't in the critical path.
		*d = []byte(s)

	default:
		return util.ErrorfSkipFrames(1, "unimplemented type for scanCol: %T", dst)
	}

	return nil
}

// Scan scans the column with the given name from the given row into dst.
func (rs *resultScanner) Scan(row sql.ResultRow, colName string, dst interface{}) error {
	idx, ok := rs.colNameToIdx[colName]
	if !ok {
		return util.Errorf("result is missing column %s", colName)
	}
	return rs.ScanIndex(row, idx, dst)
}
