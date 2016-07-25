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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/build"
	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server/serverpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/syncutil"
	"github.com/cockroachdb/cockroach/util/uuid"
)

const (
	// adminEndpoint is the prefix for RESTful endpoints used to
	// provide an administrative interface to the cockroach cluster.
	adminEndpoint = "/_admin/"
	// apiEndpoint is the prefix for the RESTful API used by the admin UI.
	apiEndpoint = adminEndpoint + "v1/"
	// healthPath is the health endpoint.
	healthPath = apiEndpoint + "health"

	// eventLimit is the maximum number of events returned by any endpoints
	// returning events.
	apiEventLimit = 1000

	// serverUIDataKeyPrefix must precede all UIData keys that are read from the
	// server.
	serverUIDataKeyPrefix = "server."
)

// apiServerMessage is the standard body for all HTTP 500 responses.
var errAdminAPIError = grpc.Errorf(codes.Internal, "An internal server error "+
	"has occurred. Please check your CockroachDB logs for more details.")

// A adminServer provides a RESTful HTTP API to administration of
// the cockroach cluster.
type adminServer struct {
	server *Server
}

// makeAdminServer allocates and returns a new REST server for
// administrative APIs.
func makeAdminServer(s *Server) adminServer {
	return adminServer{
		server: s,
	}
}

// RegisterService registers the GRPC service.
func (s *adminServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterAdminServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *adminServer) RegisterGateway(
	ctx context.Context,
	mux *gwruntime.ServeMux,
	conn *grpc.ClientConn,
) error {
	return serverpb.RegisterAdminHandler(ctx, mux, conn)
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
	log.ErrorfDepth(context.TODO(), 1, "%s", err)
	return errAdminAPIError
}

// serverErrorf logs the provided error and returns an error that should be returned by
// the RPC endpoint method.
func (s *adminServer) serverErrorf(format string, args ...interface{}) error {
	log.ErrorfDepth(context.TODO(), 1, format, args...)
	return errAdminAPIError
}

// serverErrors logs the provided errors and returns an error that should be returned by
// the RPC endpoint method.
func (s *adminServer) serverErrors(errors []error) error {
	log.ErrorfDepth(context.TODO(), 1, "%v", errors)
	return errAdminAPIError
}

// checkQueryResults performs basic tests on the provided query results and returns
// the first error that was found.
func (s *adminServer) checkQueryResults(results []sql.Result, numResults int) error {
	if a, e := len(results), numResults; a != e {
		return errors.Errorf("# of results %d != expected %d", a, e)
	}

	for _, result := range results {
		if result.Err != nil {
			return errors.Errorf("%s", result.Err)
		}
	}

	return nil
}

// firstNotFoundError returns the first table/database not found error in the
// provided results.
func (s *adminServer) firstNotFoundError(results []sql.Result) error {
	for _, res := range results {
		// TODO(cdo): Replace this crude suffix-matching with something more structured once we have
		// more structured errors.
		if res.Err != nil && strings.HasSuffix(res.Err.Error(), "does not exist") {
			return res.Err
		}
	}

	return nil
}

// Databases is an endpoint that returns a list of databases.
func (s *adminServer) Databases(
	ctx context.Context, req *serverpb.DatabasesRequest,
) (*serverpb.DatabasesResponse, error) {
	args := sql.SessionArgs{User: s.getUser(req)}
	session := sql.NewSession(ctx, args, s.server.sqlExecutor, nil)
	r := s.server.sqlExecutor.ExecuteStatements(session, "SHOW DATABASES;", nil)
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return nil, s.serverError(err)
	}

	var resp serverpb.DatabasesResponse
	for _, row := range r.ResultList[0].Rows {
		dbname, ok := row.Values[0].(*parser.DString)
		if !ok {
			return nil, s.serverErrorf("type assertion failed on db name: %T", row.Values[0])
		}
		resp.Databases = append(resp.Databases, string(*dbname))
	}

	return &resp, nil
}

// DatabaseDetails is an endpoint that returns grants and a list of table names
// for the specified database.
func (s *adminServer) DatabaseDetails(
	ctx context.Context, req *serverpb.DatabaseDetailsRequest,
) (*serverpb.DatabaseDetailsResponse, error) {
	args := sql.SessionArgs{User: s.getUser(req)}
	session := sql.NewSession(ctx, args, s.server.sqlExecutor, nil)

	// Placeholders don't work with SHOW statements, so we need to manually
	// escape the database name.
	//
	// TODO(cdo): Use placeholders when they're supported by SHOW.
	escDBName := parser.Name(req.Database).String()
	query := fmt.Sprintf("SHOW GRANTS ON DATABASE %s; SHOW TABLES FROM %s;", escDBName, escDBName)
	r := s.server.sqlExecutor.ExecuteStatements(session, query, nil)
	if err := s.firstNotFoundError(r.ResultList); err != nil {
		return nil, grpc.Errorf(codes.NotFound, "%s", err)
	}
	if err := s.checkQueryResults(r.ResultList, 2); err != nil {
		return nil, s.serverError(err)
	}

	// Marshal grants.
	var resp serverpb.DatabaseDetailsResponse
	{
		const (
			userCol       = "User"
			privilegesCol = "Privileges"
		)

		scanner := makeResultScanner(r.ResultList[0].Columns)
		for _, row := range r.ResultList[0].Rows {
			// Marshal grant, splitting comma-separated privileges into a proper slice.
			var grant serverpb.DatabaseDetailsResponse_Grant
			var privileges string
			if err := scanner.Scan(row, userCol, &grant.User); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, privilegesCol, &privileges); err != nil {
				return nil, err
			}
			grant.Privileges = strings.Split(privileges, ",")
			resp.Grants = append(resp.Grants, grant)
		}
	}

	// Marshal table names.
	{
		const tableCol = "Table"
		scanner := makeResultScanner(r.ResultList[1].Columns)
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
func (s *adminServer) TableDetails(
	ctx context.Context, req *serverpb.TableDetailsRequest,
) (*serverpb.TableDetailsResponse, error) {
	args := sql.SessionArgs{User: s.getUser(req)}
	session := sql.NewSession(ctx, args, s.server.sqlExecutor, nil)

	// TODO(cdo): Use real placeholders for the table and database names when we've extended our SQL
	// grammar to allow that.
	escDBName := parser.Name(req.Database).String()
	escTableName := parser.Name(req.Table).String()
	escQualTable := fmt.Sprintf("%s.%s", escDBName, escTableName)
	query := fmt.Sprintf("SHOW COLUMNS FROM %s; SHOW INDEX FROM %s; SHOW GRANTS ON TABLE %s; SHOW CREATE TABLE %s;",
		escQualTable, escQualTable, escQualTable, escQualTable)
	r := s.server.sqlExecutor.ExecuteStatements(session, query, nil)
	if err := s.firstNotFoundError(r.ResultList); err != nil {
		return nil, grpc.Errorf(codes.NotFound, "%s", err)
	}
	if err := s.checkQueryResults(r.ResultList, 4); err != nil {
		return nil, err
	}

	var resp serverpb.TableDetailsResponse

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
		scanner := makeResultScanner(r.ResultList[0].Columns)
		for _, row := range r.ResultList[0].Rows {
			var col serverpb.TableDetailsResponse_Column
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
				if err := scanner.Scan(row, defaultCol, &col.DefaultValue); err != nil {
					return nil, err
				}
			}
			resp.Columns = append(resp.Columns, col)
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
		scanner := makeResultScanner(r.ResultList[1].Columns)
		for _, row := range r.ResultList[1].Rows {
			// Marshal grant, splitting comma-separated privileges into a proper slice.
			var index serverpb.TableDetailsResponse_Index
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
			resp.Indexes = append(resp.Indexes, index)
		}
	}

	// Marshal SHOW GRANTS result.
	{
		const (
			userCol       = "User"
			privilegesCol = "Privileges"
		)
		scanner := makeResultScanner(r.ResultList[2].Columns)
		for _, row := range r.ResultList[2].Rows {
			// Marshal grant, splitting comma-separated privileges into a proper slice.
			var grant serverpb.TableDetailsResponse_Grant
			var privileges string
			if err := scanner.Scan(row, userCol, &grant.User); err != nil {
				return nil, err
			}
			if err := scanner.Scan(row, privilegesCol, &privileges); err != nil {
				return nil, err
			}
			grant.Privileges = strings.Split(privileges, ",")
			resp.Grants = append(resp.Grants, grant)
		}
	}

	// Marshal SHOW CREATE TABLE result.
	{
		const createTableCol = "CreateTable"
		showResult := r.ResultList[3]
		if len(showResult.Rows) != 1 {
			return nil, s.serverErrorf("CreateTable response not available.")
		}

		scanner := makeResultScanner(showResult.Columns)
		var createStmt string
		if err := scanner.Scan(showResult.Rows[0], createTableCol, &createStmt); err != nil {
			return nil, err
		}

		resp.CreateTableStatement = createStmt
	}

	// Get the number of ranges in the table. We get the key span for the table
	// data. Then, we count the number of ranges that make up that key span.
	{
		var iexecutor sql.InternalExecutor
		var tableSpan roachpb.Span
		if err := s.server.db.Txn(func(txn *client.Txn) error {
			var err error
			tableSpan, err = iexecutor.GetTableSpan(s.getUser(req), txn, escDBName, escTableName)
			return err
		}); err != nil {
			return nil, s.serverError(err)
		}
		tableRSpan := roachpb.RSpan{}
		var err error
		tableRSpan.Key, err = keys.Addr(tableSpan.Key)
		if err != nil {
			return nil, s.serverError(err)
		}
		tableRSpan.EndKey, err = keys.Addr(tableSpan.EndKey)
		if err != nil {
			return nil, s.serverError(err)
		}
		rangeCount, err := s.server.distSender.CountRanges(tableRSpan)
		if err != nil {
			return nil, s.serverError(err)
		}
		resp.RangeCount = rangeCount
	}

	// Query the zone configuration for this table.
	{
		path, err := s.queryDescriptorIDPath(session, []string{escDBName, escTableName})
		if err != nil {
			return nil, s.serverError(err)
		}

		id, zone, zoneExists, err := s.queryZonePath(session, path)
		if err != nil {
			return nil, s.serverError(err)
		}

		if !zoneExists {
			zone = config.DefaultZoneConfig()
		}
		resp.ZoneConfig = zone

		switch id {
		case path[1]:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_DATABASE
		case path[2]:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_TABLE
		default:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_CLUSTER
		}
	}

	return &resp, nil
}

// TableStats is an endpoint that returns columns, indices, and other
// relevant details for the specified table.
func (s *adminServer) TableStats(ctx context.Context, req *serverpb.TableStatsRequest) (
	*serverpb.TableStatsResponse, error,
) {
	// Get table span.
	var tableSpan roachpb.Span
	var iexecutor sql.InternalExecutor
	if err := s.server.db.Txn(func(txn *client.Txn) error {
		var err error
		tableSpan, err = iexecutor.GetTableSpan(s.getUser(req), txn, req.Database, req.Table)
		return err
	}); err != nil {
		return nil, s.serverError(err)
	}

	startKey, err := keys.Addr(tableSpan.Key)
	if err != nil {
		return nil, s.serverError(err)
	}
	endKey, err := keys.Addr(tableSpan.EndKey)
	if err != nil {
		return nil, s.serverError(err)
	}

	// Get current range descriptors for table. This is done by scanning over
	// meta2 keys for the range.
	rangeDescKVs, err := s.server.db.Scan(keys.RangeMetaKey(startKey), keys.RangeMetaKey(endKey), 0)
	if err != nil {
		return nil, s.serverError(err)
	}

	// Extract a list of node IDs from the response.
	nodeIDs := make(map[roachpb.NodeID]struct{})
	for _, kv := range rangeDescKVs {
		var rng roachpb.RangeDescriptor
		if err := kv.Value.GetProto(&rng); err != nil {
			return nil, s.serverError(err)
		}
		for _, repl := range rng.Replicas {
			nodeIDs[repl.NodeID] = struct{}{}
		}
	}

	// Construct TableStatsResponse by sending an RPC to every node involved.
	tableStatResponse := serverpb.TableStatsResponse{
		NodeCount:  int64(len(nodeIDs)),
		RangeCount: int64(len(rangeDescKVs)),
	}
	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.SpanStatsResponse
		err    error
	}

	// Send a SpanStats query to each node. Set a timeout on the context for
	// these queries.
	responses := make(chan nodeResponse)
	ctx, cancel := context.WithTimeout(ctx, base.NetworkTimeout)
	defer cancel()
	for nodeID := range nodeIDs {
		nodeID := nodeID
		if err := s.server.stopper.RunAsyncTask(func() {
			var spanResponse *serverpb.SpanStatsResponse
			client, err := s.server.status.dialNode(nodeID)
			if err == nil {
				req := serverpb.SpanStatsRequest{
					StartKey: startKey,
					EndKey:   endKey,
					NodeID:   nodeID.String(),
				}
				spanResponse, err = client.SpanStats(ctx, &req)
			}

			response := nodeResponse{
				nodeID: nodeID,
				resp:   spanResponse,
				err:    err,
			}
			select {
			case responses <- response:
				// Response processed.
			case <-ctx.Done():
				// Context completed, response no longer needed.
			}
		}); err != nil {
			return nil, err
		}
	}
	for remainingResponses := len(nodeIDs); remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			// For nodes which returned an error, note that the node's data
			// is missing. For successful calls, aggregate statistics.
			if resp.err != nil {
				tableStatResponse.MissingNodes = append(
					tableStatResponse.MissingNodes,
					serverpb.TableStatsResponse_MissingNode{
						NodeID:       resp.nodeID.String(),
						ErrorMessage: resp.err.Error(),
					},
				)
			} else {
				tableStatResponse.Stats.Add(resp.resp.TotalStats)
				tableStatResponse.ReplicaCount += int64(resp.resp.RangeCount)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return &tableStatResponse, nil
}

// Users returns a list of users, stripped of any passwords.
func (s *adminServer) Users(ctx context.Context, req *serverpb.UsersRequest) (*serverpb.UsersResponse, error) {
	args := sql.SessionArgs{User: s.getUser(req)}
	session := sql.NewSession(ctx, args, s.server.sqlExecutor, nil)
	query := "SELECT username FROM system.users"
	r := s.server.sqlExecutor.ExecuteStatements(session, query, nil)
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return nil, s.serverError(err)
	}

	var resp serverpb.UsersResponse
	for _, row := range r.ResultList[0].Rows {
		resp.Users = append(resp.Users, serverpb.UsersResponse_User{Username: string(*row.Values[0].(*parser.DString))})
	}
	return &resp, nil
}

// Events is an endpoint that returns the latest event log entries, with the following
// optional URL parameters:
//
// type=STRING  returns events with this type (e.g. "create_table")
// targetID=INT returns events for that have this targetID
func (s *adminServer) Events(ctx context.Context, req *serverpb.EventsRequest) (*serverpb.EventsResponse, error) {
	args := sql.SessionArgs{User: s.getUser(req)}
	session := sql.NewSession(ctx, args, s.server.sqlExecutor, nil)

	// Execute the query.
	q := makeSQLQuery()
	q.Append("SELECT timestamp, eventType, targetID, reportingID, info, uniqueID ")
	q.Append("FROM system.eventlog ")
	q.Append("WHERE true ") // This simplifies the WHERE clause logic below.
	if len(req.Type) > 0 {
		q.Append("AND eventType = $ ", parser.NewDString(req.Type))
	}
	if req.TargetId > 0 {
		q.Append("AND targetID = $ ", parser.NewDInt(parser.DInt(req.TargetId)))
	}
	q.Append("ORDER BY timestamp DESC ")
	q.Append("LIMIT $", parser.NewDInt(parser.DInt(apiEventLimit)))
	if len(q.Errors()) > 0 {
		return nil, s.serverErrors(q.Errors())
	}
	r := s.server.sqlExecutor.ExecuteStatements(session, q.String(), q.QueryArguments())
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return nil, s.serverError(err)
	}

	// Marshal response.
	var resp serverpb.EventsResponse
	scanner := makeResultScanner(r.ResultList[0].Columns)
	for _, row := range r.ResultList[0].Rows {
		var event serverpb.EventsResponse_Event
		var ts time.Time
		if err := scanner.ScanIndex(row, 0, &ts); err != nil {
			return nil, err
		}
		event.Timestamp = serverpb.EventsResponse_Event_Timestamp{Sec: ts.Unix(), Nsec: uint32(ts.Nanosecond())}
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

		resp.Events = append(resp.Events, event)
	}
	return &resp, nil
}

// getUIData returns the values and timestamps for the given UI keys. Keys
// that are not found will not be returned.
func (s *adminServer) getUIData(session *sql.Session, user string, keys []string) (*serverpb.GetUIDataResponse, error) {
	if len(keys) == 0 {
		return &serverpb.GetUIDataResponse{}, nil
	}

	// Query database.
	query := makeSQLQuery()
	query.Append("SELECT key, value, lastUpdated FROM system.ui WHERE key IN (")
	for i, key := range keys {
		if i != 0 {
			query.Append(",")
		}
		query.Append("$", parser.NewDString(key))
	}
	query.Append(");")
	if err := query.Errors(); err != nil {
		return nil, s.serverErrorf("error constructing query: %v", err)
	}
	r := s.server.sqlExecutor.ExecuteStatements(session, query.String(), query.QueryArguments())
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return nil, s.serverError(err)
	}

	// Marshal results.
	resp := serverpb.GetUIDataResponse{KeyValues: make(map[string]serverpb.GetUIDataResponse_Value)}
	for _, row := range r.ResultList[0].Rows {
		dKey, ok := row.Values[0].(*parser.DString)
		if !ok {
			return nil, s.serverErrorf("unexpected type for UI key: %T", row.Values[0])
		}
		dValue, ok := row.Values[1].(*parser.DBytes)
		if !ok {
			return nil, s.serverErrorf("unexpected type for UI value: %T", row.Values[1])
		}
		dLastUpdated, ok := row.Values[2].(*parser.DTimestamp)
		if !ok {
			return nil, s.serverErrorf("unexpected type for UI lastUpdated: %T", row.Values[2])
		}

		resp.KeyValues[string(*dKey)] = serverpb.GetUIDataResponse_Value{
			Value:       []byte(*dValue),
			LastUpdated: serverpb.GetUIDataResponse_Timestamp{Sec: dLastUpdated.Unix(), Nsec: uint32(dLastUpdated.Nanosecond())},
		}
	}
	return &resp, nil
}

// SetUIData is an endpoint that stores the given key/value pairs in the
// system.ui table. See GetUIData for more details on semantics.
func (s *adminServer) SetUIData(ctx context.Context, req *serverpb.SetUIDataRequest) (*serverpb.SetUIDataResponse, error) {
	if len(req.KeyValues) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "KeyValues cannot be empty")
	}

	args := sql.SessionArgs{User: s.getUser(req)}
	session := sql.NewSession(ctx, args, s.server.sqlExecutor, nil)

	for key, val := range req.KeyValues {
		// Do an upsert of the key. We update each key in a separate transaction to
		// avoid long-running transactions and possible deadlocks.
		br := s.server.sqlExecutor.ExecuteStatements(session, "BEGIN;", nil)
		if err := s.checkQueryResults(br.ResultList, 1); err != nil {
			return nil, s.serverError(err)
		}

		// See if the key already exists.
		resp, err := s.getUIData(session, s.getUser(req), []string{key})
		if err != nil {
			return nil, s.serverError(err)
		}
		_, alreadyExists := resp.KeyValues[key]

		// INSERT or UPDATE as appropriate.
		if alreadyExists {
			query := "UPDATE system.ui SET value = $1, lastUpdated = NOW() WHERE key = $2; COMMIT;"
			qargs := parser.NewPlaceholderInfo()
			qargs.SetValue(`1`, parser.NewDString(string(val)))
			qargs.SetValue(`2`, parser.NewDString(key))
			r := s.server.sqlExecutor.ExecuteStatements(session, query, qargs)
			if err := s.checkQueryResults(r.ResultList, 2); err != nil {
				return nil, s.serverError(err)
			}
			if a, e := r.ResultList[0].RowsAffected, 1; a != e {
				return nil, s.serverErrorf("rows affected %d != expected %d", a, e)
			}
		} else {
			query := "INSERT INTO system.ui (key, value, lastUpdated) VALUES ($1, $2, NOW()); COMMIT;"
			qargs := parser.NewPlaceholderInfo()
			qargs.SetValue(`1`, parser.NewDString(key))
			qargs.SetValue(`2`, parser.NewDBytes(parser.DBytes(val)))
			r := s.server.sqlExecutor.ExecuteStatements(session, query, qargs)
			if err := s.checkQueryResults(r.ResultList, 2); err != nil {
				return nil, s.serverError(err)
			}
			if a, e := r.ResultList[0].RowsAffected, 1; a != e {
				return nil, s.serverErrorf("rows affected %d != expected %d", a, e)
			}
		}
	}

	return &serverpb.SetUIDataResponse{}, nil
}

// GetUIData returns data associated with the given keys, which was stored
// earlier through SetUIData.
//
// The stored values are meant to be opaque to the server. In the rare case that
// the server code needs to call this method, it should only read from keys that
// have the prefix `serverUIDataKeyPrefix`.
func (s *adminServer) GetUIData(ctx context.Context, req *serverpb.GetUIDataRequest) (*serverpb.GetUIDataResponse, error) {
	args := sql.SessionArgs{User: s.getUser(req)}
	session := sql.NewSession(ctx, args, s.server.sqlExecutor, nil)

	if len(req.Keys) == 0 {
		return nil, grpc.Errorf(codes.InvalidArgument, "keys cannot be empty")
	}

	resp, err := s.getUIData(session, s.getUser(req), req.Keys)
	if err != nil {
		return nil, s.serverError(err)
	}

	return resp, nil
}

// Cluster returns cluster metadata.
func (s *adminServer) Cluster(_ context.Context, req *serverpb.ClusterRequest) (*serverpb.ClusterResponse, error) {
	clusterID := s.server.node.ClusterID
	if uuid.Equal(clusterID, *uuid.EmptyUUID) {
		return nil, grpc.Errorf(codes.Unavailable, "cluster ID not yet available")
	}
	return &serverpb.ClusterResponse{ClusterID: clusterID.String()}, nil
}

func (s *adminServer) Health(ctx context.Context, req *serverpb.HealthRequest) (*serverpb.HealthResponse, error) {
	return &serverpb.HealthResponse{}, nil
}

func (s *adminServer) Drain(req *serverpb.DrainRequest, stream serverpb.Admin_DrainServer) error {
	on := make([]serverpb.DrainMode, len(req.On))
	for i := range req.On {
		on[i] = serverpb.DrainMode(req.On[i])
	}
	off := make([]serverpb.DrainMode, len(req.Off))
	for i := range req.Off {
		off[i] = serverpb.DrainMode(req.Off[i])
	}

	_ = s.server.Undrain(off)

	nowOn, err := s.server.Drain(on)
	if err != nil {
		return err
	}

	res := serverpb.DrainResponse{
		On: make([]int32, len(nowOn)),
	}
	for i := range nowOn {
		res.On[i] = int32(nowOn[i])
	}
	if err := stream.Send(&res); err != nil {
		return err
	}

	if req.Shutdown {
		s.server.grpc.Stop()
		go s.server.stopper.Stop()
	}

	ctx := stream.Context()

	select {
	case <-s.server.stopper.IsStopped():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitForStoreFrozen polls the given stores until they all report having no
// unfrozen Replicas (or an error or timeout occurs).
func (s *adminServer) waitForStoreFrozen(
	stream serverpb.Admin_ClusterFreezeServer,
	stores map[roachpb.StoreID]roachpb.NodeID,
	wantFrozen bool,
) error {
	mu := struct {
		syncutil.Mutex
		oks map[roachpb.StoreID]bool
	}{
		oks: make(map[roachpb.StoreID]bool),
	}

	opts := base.DefaultRetryOptions()
	opts.Closer = s.server.stopper.ShouldQuiesce()
	opts.MaxRetries = 20
	sem := make(chan struct{}, 256)
	errChan := make(chan error, 1)
	sendErr := func(err error) {
		select {
		case errChan <- err:
		default:
		}
	}

	numWaiting := len(stores) // loop until this drops to zero
	var err error
	for r := retry.Start(opts); r.Next(); {
		mu.Lock()
		for storeID, nodeID := range stores {
			storeID, nodeID := storeID, nodeID // loop-local copies for goroutine
			var nodeDesc roachpb.NodeDescriptor
			if err := s.server.gossip.GetInfoProto(gossip.MakeNodeIDKey(nodeID), &nodeDesc); err != nil {
				sendErr(err)
				break
			}
			addr := nodeDesc.Address.String()

			if _, inflightOrSucceeded := mu.oks[storeID]; inflightOrSucceeded {
				continue
			}
			mu.oks[storeID] = false // mark as inflight
			action := func() (err error) {
				var resp *roachpb.PollFrozenResponse
				defer func() {
					message := fmt.Sprintf("node %d, store %d: ", nodeID, storeID)

					if err != nil {
						message += err.Error()
					} else {
						numMismatching := len(resp.Results)
						mu.Lock()
						if numMismatching == 0 {
							// If the Store is in the right state, mark it as such.
							// This means we won't try it again.
							message += "ready"
							mu.oks[storeID] = true
						} else {
							// Otherwise, forget that we tried the Store so that
							// the retry loop picks it up again.
							message += fmt.Sprintf("%d replicas report wrong status", numMismatching)
							if limit := 10; numMismatching > limit {
								message += " [truncated]: "
								resp.Results = resp.Results[:limit]
							} else {
								message += ": "
							}
							message += fmt.Sprintf("%+v", resp.Results)
							delete(mu.oks, storeID)
						}
						mu.Unlock()
						err = stream.Send(&serverpb.ClusterFreezeResponse{
							Message: message,
						})
					}
				}()
				conn, err := s.server.rpcContext.GRPCDial(addr)
				if err != nil {
					return err
				}
				client := roachpb.NewInternalStoresClient(conn)
				resp, err = client.PollFrozen(context.Background(),
					&roachpb.PollFrozenRequest{
						StoreRequestHeader: roachpb.StoreRequestHeader{
							NodeID:  nodeID,
							StoreID: storeID,
						},
						// If we are looking to freeze everything, we want to
						// collect thawed Replicas, and vice versa.
						CollectFrozen: !wantFrozen,
					})
				return err
			}
			// Run a limited, non-blocking task. That means the task simply
			// won't run if the semaphore is full (or the node is draining).
			// Both are handled by the surrounding retry loop.
			if err := s.server.stopper.RunLimitedAsyncTask(sem, func() {
				if err := action(); err != nil {
					sendErr(err)
				}
			}); err != nil {
				// Node draining.
				sendErr(err)
				break
			}
		}

		numWaiting = len(stores)
		for _, ok := range mu.oks {
			if ok {
				// Store has reported that it is frozen.
				numWaiting--
				continue
			}
		}
		mu.Unlock()

		select {
		case err = <-errChan:
		default:
		}

		// Keep going unless there's been an error or everyone's frozen.
		if err != nil || numWaiting == 0 {
			break
		}
		if err := stream.Send(&serverpb.ClusterFreezeResponse{
			Message: fmt.Sprintf("waiting for %d store%s to apply operation",
				numWaiting, util.Pluralize(int64(numWaiting))),
		}); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	if numWaiting > 0 {
		err = fmt.Errorf("timed out waiting for %d store%s to report freeze",
			numWaiting, util.Pluralize(int64(numWaiting)))
	}
	return err
}

func (s *adminServer) ClusterFreeze(
	req *serverpb.ClusterFreezeRequest,
	stream serverpb.Admin_ClusterFreezeServer,
) error {
	var totalAffected int64
	stores := make(map[roachpb.StoreID]roachpb.NodeID)
	process := func(from, to roachpb.Key) (roachpb.Key, error) {
		b := &client.Batch{}
		fa := roachpb.NewChangeFrozen(from, to, req.Freeze, build.GetInfo().Tag)
		b.AddRawRequest(fa)
		if err := s.server.db.Run(b); err != nil {
			return nil, err
		}
		fr := b.RawResponse().Responses[0].GetInner().(*roachpb.ChangeFrozenResponse)
		totalAffected += fr.RangesAffected
		for storeID, nodeID := range fr.Stores {
			stores[storeID] = nodeID
		}
		return fr.MinStartKey.AsRawKey(), nil
	}

	task := "thaw"
	if req.Freeze {
		task = "freeze"
		// When freezing, we save the meta2 and meta1 range for last to avoid
		// interfering with command routing.
		// Note that we freeze only Ranges whose StartKey is included. In
		// particular, a Range which contains some meta keys will not be frozen
		// by the request that begins at Meta2KeyMax. ChangeFreeze gives us the
		// leftmost covered Range back, which we use for the next request to
		// avoid split-related races.
		freezeTo := roachpb.KeyMax // updated as we go along
		freezeFroms := []roachpb.Key{
			keys.Meta2KeyMax, // freeze userspace
			keys.Meta1KeyMax, // freeze all meta2 ranges
			keys.LocalMax,    // freeze first range (meta1)
		}

		for _, freezeFrom := range freezeFroms {
			var err error
			if freezeTo, err = process(freezeFrom, freezeTo); err != nil {
				return err
			}
		}
	} else {
		// When unfreezing, we walk in opposite order and try the first range
		// first. We should be able to get there if the first range manages to
		// gossip. From that, we can talk to the second level replicas, and
		// then to everyone else. Because ChangeFrozen works in forward order,
		// we can simply hit the whole keyspace at once.
		// TODO(tschottdorf): make the first range replicas gossip their
		// descriptor unconditionally or we won't always be able to unfreeze
		// (except by restarting a node which holds the first range).
		if _, err := process(keys.LocalMax, roachpb.KeyMax); err != nil {
			return err
		}
	}
	if err := stream.Send(&serverpb.ClusterFreezeResponse{
		RangesAffected: totalAffected,
		Message:        fmt.Sprintf("proposed %s to %d ranges", task, totalAffected),
	}); err != nil {
		return err
	}
	return s.waitForStoreFrozen(stream, stores, req.Freeze)
}

// sqlQuery allows you to incrementally build a SQL query that uses
// placeholders. Instead of specific placeholders like $1, you instead use the
// temporary placeholder $.
type sqlQuery struct {
	buf   bytes.Buffer
	pidx  int
	qargs parser.PlaceholderInfo
	errs  []error
}

func makeSQLQuery() *sqlQuery {
	res := &sqlQuery{}
	res.qargs.Clear()
	return res
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

// QueryArguments returns a filled map of placeholders containing all arguments
// provided to this query through Append.
func (q *sqlQuery) QueryArguments() *parser.PlaceholderInfo {
	return &q.qargs
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
	firstpidx := q.pidx
	for _, r := range s {
		q.buf.WriteRune(r)
		if r == '$' {
			q.pidx++
			placeholders++
			q.buf.WriteString(strconv.Itoa(q.pidx)) // SQL placeholders are 1-based
		}
	}

	if placeholders != len(params) {
		q.errs = append(q.errs,
			errors.Errorf("# of placeholders %d != # of params %d", placeholders, len(params)))
	}
	for i, param := range params {
		q.qargs.SetValue(fmt.Sprint(firstpidx+i+1), param)
	}
}

// resultScanner scans columns from sql.ResultRow instances into variables,
// performing the appropriate casting and error detection along the way.
type resultScanner struct {
	colNameToIdx map[string]int
}

func makeResultScanner(cols []sql.ResultColumn) resultScanner {
	rs := resultScanner{
		colNameToIdx: make(map[string]int),
	}
	for i, col := range cols {
		rs.colNameToIdx[col.Name] = i
	}
	return rs
}

// IsNull returns whether the specified column of the given row contains
// a SQL NULL value.
func (rs resultScanner) IsNull(row sql.ResultRow, col string) (bool, error) {
	idx, ok := rs.colNameToIdx[col]
	if !ok {
		return false, errors.Errorf("result is missing column %s", col)
	}
	return row.Values[idx] == parser.DNull, nil
}

// ScanIndex scans the given column index of the given row into dst.
func (rs resultScanner) ScanIndex(row sql.ResultRow, index int, dst interface{}) error {
	src := row.Values[index]

	switch d := dst.(type) {
	case *string:
		if dst == nil {
			return errors.Errorf("nil destination pointer passed in")
		}
		s, ok := src.(*parser.DString)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = string(*s)

	case *bool:
		if dst == nil {
			return errors.Errorf("nil destination pointer passed in")
		}
		s, ok := src.(*parser.DBool)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = bool(*s)

	case *int64:
		if dst == nil {
			return errors.Errorf("nil destination pointer passed in")
		}
		s, ok := src.(*parser.DInt)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = int64(*s)

	case *time.Time:
		if dst == nil {
			return errors.Errorf("nil destination pointer passed in")
		}
		s, ok := src.(*parser.DTimestamp)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = s.Time

	case *[]byte:
		if dst == nil {
			return errors.Errorf("nil destination pointer passed in")
		}
		s, ok := src.(*parser.DBytes)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		// Yes, this copies, but this probably isn't in the critical path.
		*d = []byte(*s)

	default:
		return errors.Errorf("unimplemented type for scanCol: %T", dst)
	}

	return nil
}

// Scan scans the column with the given name from the given row into dst.
func (rs resultScanner) Scan(row sql.ResultRow, colName string, dst interface{}) error {
	idx, ok := rs.colNameToIdx[colName]
	if !ok {
		return errors.Errorf("result is missing column %s", colName)
	}
	return rs.ScanIndex(row, idx, dst)
}

// TODO(mrtracy): The following methods, used to look up the zone configuration
// for a database or table, use the same algorithm as a set of methods in
// cli/zone.go for the same purpose. However, as that code connects to the
// server with a SQL connections, while this code uses a sql.Executor directly,
// the code cannot be commonized.
//
// Github issue #4869 is the most likely candidate for addressing this
// incompatibility; when that issue has been resolved, this code from
// cli/zone.go should be moved to a common location and shared with this system.

// queryZone retrieves the specific ZoneConfig associated with the supplied ID,
// if it exists.
func (s *adminServer) queryZone(
	session *sql.Session, id sqlbase.ID,
) (config.ZoneConfig, bool, error) {
	const query = `SELECT config FROM system.zones WHERE id = $1`
	params := parser.NewPlaceholderInfo()
	params.SetValue(`1`, parser.NewDInt(parser.DInt(id)))
	r := s.server.sqlExecutor.ExecuteStatements(session, query, params)
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return config.ZoneConfig{}, false, err
	}

	result := r.ResultList[0]
	if len(result.Rows) == 0 {
		return config.ZoneConfig{}, false, nil
	}

	var zoneBytes []byte
	scanner := resultScanner{}
	err := scanner.ScanIndex(result.Rows[0], 0, &zoneBytes)
	if err != nil {
		return config.ZoneConfig{}, false, err
	}

	var zone config.ZoneConfig
	if err := zone.Unmarshal(zoneBytes); err != nil {
		return config.ZoneConfig{}, false, err
	}
	return zone, true, nil
}

// queryZonePath queries a path of sql object IDs, as generated by
// queryDescriptorIDPath(), for a ZoneConfig. It returns the most specific
// ZoneConfig specified for the object IDs in the path.
func (s *adminServer) queryZonePath(
	session *sql.Session, path []sqlbase.ID,
) (sqlbase.ID, config.ZoneConfig, bool, error) {
	for i := len(path) - 1; i >= 0; i-- {
		zone, zoneExists, err := s.queryZone(session, path[i])
		if err != nil || zoneExists {
			return path[i], zone, true, err
		}
	}
	return 0, config.ZoneConfig{}, false, nil
}

// queryNamespaceID queries for the ID of the namespace with the given name and
// parent ID.
func (s *adminServer) queryNamespaceID(
	session *sql.Session, parentID sqlbase.ID, name string,
) (sqlbase.ID, error) {
	const query = `SELECT id FROM system.namespace WHERE parentID = $1 AND name = $2`
	params := parser.NewPlaceholderInfo()
	params.SetValue(`1`, parser.NewDInt(parser.DInt(parentID)))
	params.SetValue(`2`, parser.NewDString(name))
	r := s.server.sqlExecutor.ExecuteStatements(session, query, params)
	if err := s.checkQueryResults(r.ResultList, 1); err != nil {
		return 0, err
	}

	result := r.ResultList[0]
	if len(result.Rows) == 0 {
		return 0, errors.Errorf("namespace %s with ParentID %d not found", name, parentID)
	}

	var id int64
	scanner := resultScanner{}
	err := scanner.ScanIndex(result.Rows[0], 0, &id)
	if err != nil {
		return 0, err
	}

	return sqlbase.ID(id), nil
}

// queryDescriptorIDPath converts a path of namespaces into a path of namespace
// IDs. For example, if this function is called with a database/table name pair,
// it will return a list of IDs consisting of the root namespace ID, the
// databases ID, and the table ID (in that order).
func (s *adminServer) queryDescriptorIDPath(
	session *sql.Session, names []string,
) ([]sqlbase.ID, error) {
	path := []sqlbase.ID{keys.RootNamespaceID}
	for _, name := range names {
		id, err := s.queryNamespaceID(session, path[len(path)-1], name)
		if err != nil {
			return nil, err
		}
		path = append(path, id)
	}
	return path, nil
}
