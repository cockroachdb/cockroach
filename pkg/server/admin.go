// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	apd "github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/privchecker"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/ts/catalog"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/safesql"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingui"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	gwutil "github.com/grpc-ecosystem/grpc-gateway/utilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

// Number of empty ranges for table descriptors that aren't actually tables. These
// cause special cases in range count computations because we split along them anyway,
// but they're not SQL tables.
func nonTableDescriptorRangeCount() int64 {
	// NB: explicitly reference them for IDE usage.
	return int64(len([]int{
		keys.MetaRangesID,
		keys.SystemRangesID,
		keys.TimeseriesRangesID,
		keys.LivenessRangesID,
		keys.PublicSchemaID, // TODO(richardjcai): Remove this in 22.2.
		keys.TenantsRangesID,
	}))
}

// A adminServer provides a RESTful HTTP API to administration of
// the cockroach cluster.
type adminServer struct {
	serverpb.UnimplementedAdminServer
	log.AmbientContext

	privilegeChecker privchecker.CheckerForRPCHandlers

	internalExecutor *sql.InternalExecutor
	sqlServer        *SQLServer
	metricsRecorder  *status.MetricsRecorder
	memMonitor       *mon.BytesMonitor
	statsLimiter     *quotapool.IntPool
	st               *cluster.Settings
	serverIterator   ServerIterator
	distSender       *kvcoord.DistSender
	rpcContext       *rpc.Context
	clock            *hlc.Clock
	grpc             *grpcServer
	db               *kv.DB
	drainServer      *drainServer
}

// systemAdminServer is an extension of adminServer that implements
// certain endpoints that require system tenant access. Currently,
// these include anything involve node liveness, decommissioning,
// direct range access, etc.
// In general, add new RPC implementations to the base adminServer
// to ensure feature parity with system and app tenants.
type systemAdminServer struct {
	*adminServer

	nodeLiveness *liveness.NodeLiveness
	server       *topLevelServer
}

var tableStatsMaxFetcherConcurrency = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.admin.table_stats.max_fetcher_concurrency",
	"maximum number of concurrent table stats fetches to run",
	64, // arbitrary
	settings.PositiveInt,
)

func newSystemAdminServer(
	sqlServer *SQLServer,
	cs *cluster.Settings,
	adminAuthzCheck privchecker.CheckerForRPCHandlers,
	ie *sql.InternalExecutor,
	ambient log.AmbientContext,
	metricsRecorder *status.MetricsRecorder,
	db *kv.DB,
	nodeLiveness *liveness.NodeLiveness,
	rpcCtx *rpc.Context,
	serverIterator ServerIterator,
	clock *hlc.Clock,
	distSender *kvcoord.DistSender,
	grpc *grpcServer,
	drainServer *drainServer,
	s *topLevelServer,
) *systemAdminServer {
	adminServer := newAdminServer(
		sqlServer,
		cs,
		adminAuthzCheck,
		ie,
		ambient,
		metricsRecorder,
		db,
		rpcCtx,
		serverIterator,
		clock,
		distSender,
		grpc,
		drainServer,
	)
	return &systemAdminServer{
		adminServer:  adminServer,
		nodeLiveness: nodeLiveness,
		server:       s,
	}
}

// newAdminServer allocates and returns a new REST server for administrative
// APIs. Note that the argument s, the Server, is not yet initialized, and
// cannot be used for anything other than storing a reference for later use.
// By the time this adminServer needs to serve requests, it will have been
// initialized.
func newAdminServer(
	sqlServer *SQLServer,
	cs *cluster.Settings,
	adminAuthzCheck privchecker.CheckerForRPCHandlers,
	ie *sql.InternalExecutor,
	ambient log.AmbientContext,
	metricsRecorder *status.MetricsRecorder,
	db *kv.DB,
	rpcCtx *rpc.Context,
	serverIterator ServerIterator,
	clock *hlc.Clock,
	distSender *kvcoord.DistSender,
	grpc *grpcServer,
	drainServer *drainServer,
) *adminServer {
	server := &adminServer{
		AmbientContext:   ambient,
		privilegeChecker: adminAuthzCheck,
		internalExecutor: ie,
		sqlServer:        sqlServer,
		metricsRecorder:  metricsRecorder,
		statsLimiter: quotapool.NewIntPool(
			"table stats",
			uint64(tableStatsMaxFetcherConcurrency.Get(&cs.SV)),
		),
		st:             cs,
		serverIterator: serverIterator,
		distSender:     distSender,
		rpcContext:     rpcCtx,
		clock:          clock,
		grpc:           grpc,
		db:             db,
		drainServer:    drainServer,
	}
	tableStatsMaxFetcherConcurrency.SetOnChange(&cs.SV, func(ctx context.Context) {
		server.statsLimiter.UpdateCapacity(
			uint64(tableStatsMaxFetcherConcurrency.Get(&cs.SV)),
		)
	})

	// TODO(knz): We do not limit memory usage by admin operations
	// yet. Is this wise?
	server.memMonitor = mon.NewUnlimitedMonitor(context.Background(), mon.Options{
		Name:     mon.MakeMonitorName("admin"),
		Settings: cs,
	})
	return server
}

// RegisterService registers the GRPC service.
func (s *systemAdminServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterAdminServer(g, s)
}

// RegisterService registers the GRPC service.
func (s *adminServer) RegisterService(g *grpc.Server) {
	serverpb.RegisterAdminServer(g, s)
}

// RegisterGateway starts the gateway (i.e. reverse proxy) that proxies HTTP requests
// to the appropriate gRPC endpoints.
func (s *adminServer) RegisterGateway(
	ctx context.Context, mux *gwruntime.ServeMux, conn *grpc.ClientConn,
) error {
	// Register the /_admin/v1/stmtbundle endpoint, which serves statement support
	// bundles as files.
	stmtBundlePattern := gwruntime.MustPattern(gwruntime.NewPattern(
		1, /* version */
		[]int{
			int(gwutil.OpLitPush), 0, int(gwutil.OpLitPush), 1, int(gwutil.OpLitPush), 2,
			int(gwutil.OpPush), 0, int(gwutil.OpConcatN), 1, int(gwutil.OpCapture), 3},
		[]string{"_admin", "v1", "stmtbundle", "id"},
		"", /* verb */
	))

	mux.Handle("GET", stmtBundlePattern, func(
		w http.ResponseWriter, req *http.Request, pathParams map[string]string,
	) {
		idStr, ok := pathParams["id"]
		if !ok {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		id, err := strconv.ParseInt(idStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}

		// The privilege checks in the privilege checker below checks the user in the incoming
		// gRPC metadata.
		md := authserver.TranslateHTTPAuthInfoToGRPCMetadata(req.Context(), req)
		authCtx := metadata.NewIncomingContext(req.Context(), md)
		authCtx = s.AnnotateCtx(authCtx)
		if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(authCtx); err != nil {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		s.getStatementBundle(req.Context(), id, w)
	})

	// Register the endpoints defined in the proto.
	return serverpb.RegisterAdminHandler(ctx, mux, conn)
}

// isNotFoundError returns true if err is a table/database not found error.
func isNotFoundError(err error) bool {
	// TODO(cdo): Replace this crude suffix-matching with something more structured once we have
	// more structured errors.
	return err != nil && strings.HasSuffix(err.Error(), "does not exist")
}

// AllMetricMetadata returns all metrics' metadata.
func (s *adminServer) AllMetricMetadata(
	ctx context.Context, req *serverpb.MetricMetadataRequest,
) (*serverpb.MetricMetadataResponse, error) {

	md, _, _ := s.metricsRecorder.GetMetricsMetadata(true /* combine */)
	metricNames := s.metricsRecorder.GetRecordedMetricNames(md)
	resp := &serverpb.MetricMetadataResponse{
		Metadata:      md,
		RecordedNames: metricNames,
	}

	return resp, nil
}

// ChartCatalog returns a catalog of Admin UI charts useful for debugging.
func (s *adminServer) ChartCatalog(
	ctx context.Context, req *serverpb.ChartCatalogRequest,
) (*serverpb.ChartCatalogResponse, error) {
	nodeMd, appMd, srvMd := s.metricsRecorder.GetMetricsMetadata(false /* combine */)

	chartCatalog, err := catalog.GenerateCatalog(nodeMd, appMd, srvMd)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	resp := &serverpb.ChartCatalogResponse{
		Catalog: chartCatalog,
	}

	return resp, nil
}

// Databases is an endpoint that returns a list of databases.
func (s *adminServer) Databases(
	ctx context.Context, req *serverpb.DatabasesRequest,
) (_ *serverpb.DatabasesResponse, retErr error) {
	ctx = s.AnnotateCtx(ctx)

	sessionUser, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	if err := s.privilegeChecker.RequireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	r, err := s.databasesHelper(ctx, req, sessionUser, 0, 0)
	return r, maybeHandleNotFoundError(ctx, err)
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) databasesHelper(
	ctx context.Context,
	req *serverpb.DatabasesRequest,
	sessionUser username.SQLUsername,
	limit, offset int,
) (_ *serverpb.DatabasesResponse, retErr error) {
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-dbs", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sessionUser},
		"SHOW DATABASES",
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	var resp serverpb.DatabasesResponse
	var hasNext bool
	for hasNext, err = it.Next(ctx); hasNext; hasNext, err = it.Next(ctx) {
		row := it.Cur()
		dbDatum, ok := tree.AsDString(row[0])
		if !ok {
			return nil, errors.Errorf("type assertion failed on db name: %T", row[0])
		}
		dbName := string(dbDatum)
		resp.Databases = append(resp.Databases, dbName)
	}
	if err != nil {
		return nil, err
	}

	return &resp, nil
}

// maybeHandleNotFoundError checks the provided error and
// conditionally returns a gRPC NotFound code.
// It returns a gRPC error in any case.
func maybeHandleNotFoundError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if isNotFoundError(err) {
		return grpcstatus.Errorf(codes.NotFound, "%s", err)
	}
	return srverrors.ServerError(ctx, err)
}

// DatabaseDetails is an endpoint that returns grants and a list of table names
// for the specified database.
func (s *adminServer) DatabaseDetails(
	ctx context.Context, req *serverpb.DatabaseDetailsRequest,
) (_ *serverpb.DatabaseDetailsResponse, retErr error) {
	ctx = s.AnnotateCtx(ctx)
	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	if err := s.privilegeChecker.RequireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	r, err := s.databaseDetailsHelper(ctx, req, userName)
	return r, maybeHandleNotFoundError(ctx, err)
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) getDatabaseGrants(
	ctx context.Context,
	req *serverpb.DatabaseDetailsRequest,
	userName username.SQLUsername,
	limit, offset int,
) (resp []serverpb.DatabaseDetailsResponse_Grant, retErr error) {
	escDBName := tree.NameStringP(&req.Database)
	// Placeholders don't work with SHOW statements, so we need to manually
	// escape the database name.
	//
	// TODO(cdo): Use placeholders when they're supported by SHOW.

	// Marshal grants.
	query := safesql.NewQuery()
	// We use Sprintf instead of the more canonical query argument approach, as
	// that doesn't support arguments inside a SHOW subquery yet.
	query.Append(fmt.Sprintf("SELECT * FROM [SHOW GRANTS ON DATABASE %s]", escDBName))
	if limit > 0 {
		query.Append(" LIMIT $", limit)
		if offset > 0 {
			query.Append(" OFFSET $", offset)
		}
	}
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-grants", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		// We only want to show the grants on the database.
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)
	{
		const (
			userCol       = "grantee"
			privilegesCol = "privilege_type"
		)

		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			// If ok == false, the query returned 0 rows.
			scanner := makeResultScanner(it.Types())
			for ; ok; ok, err = it.Next(ctx) {
				row := it.Cur()
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
				resp = append(resp, grant)
			}
			if err = maybeHandleNotFoundError(ctx, err); err != nil {
				return nil, err
			}
		}
	}
	return resp, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) getDatabaseTables(
	ctx context.Context,
	req *serverpb.DatabaseDetailsRequest,
	userName username.SQLUsername,
	limit, offset int,
) (resp []string, retErr error) {
	query := safesql.NewQuery()
	query.Append(`SELECT table_schema, table_name FROM information_schema.tables
WHERE table_catalog = $ AND table_type != 'SYSTEM VIEW'`, req.Database)
	query.Append(" ORDER BY table_name")
	if limit > 0 {
		query.Append(" LIMIT $", limit)
		if offset > 0 {
			query.Append(" OFFSET $", offset)
		}
	}
	// Marshal table names.
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-tables", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName, Database: req.Database},
		query.String(), query.QueryArguments()...)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)
	{
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}

		if ok {
			// If ok == false, the query returned 0 rows.
			scanner := makeResultScanner(it.Types())
			for ; ok; ok, err = it.Next(ctx) {
				row := it.Cur()
				var schemaName, tableName string
				if err := scanner.Scan(row, "table_schema", &schemaName); err != nil {
					return nil, err
				}
				if err := scanner.Scan(row, "table_name", &tableName); err != nil {
					return nil, err
				}
				resp = append(resp, fmt.Sprintf("%s.%s",
					tree.NameStringP(&schemaName), tree.NameStringP(&tableName)))
			}
			if err != nil {
				return nil, err
			}
		}
	}
	return resp, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) getMiscDatabaseDetails(
	ctx context.Context,
	req *serverpb.DatabaseDetailsRequest,
	userName username.SQLUsername,
	resp *serverpb.DatabaseDetailsResponse,
) (*serverpb.DatabaseDetailsResponse, error) {
	if resp == nil {
		resp = &serverpb.DatabaseDetailsResponse{}
	}
	// Query the descriptor ID and zone configuration for this database.
	databaseID, err := s.queryDatabaseID(ctx, userName, req.Database)
	if err != nil {
		return nil, err
	}
	resp.DescriptorID = int64(databaseID)

	id, zone, zoneExists, err := s.queryZonePath(ctx, userName, []descpb.ID{databaseID})
	if err != nil {
		return nil, err
	}

	if !zoneExists {
		zone = s.sqlServer.cfg.DefaultZoneConfig
	}
	resp.ZoneConfig = zone

	switch id {
	case databaseID:
		resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_DATABASE
	default:
		resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_CLUSTER
	}
	return resp, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) databaseDetailsHelper(
	ctx context.Context, req *serverpb.DatabaseDetailsRequest, userName username.SQLUsername,
) (_ *serverpb.DatabaseDetailsResponse, retErr error) {
	var resp serverpb.DatabaseDetailsResponse
	var err error

	resp.Grants, err = s.getDatabaseGrants(ctx, req, userName, 0, 0)
	if err != nil {
		return nil, err
	}
	resp.TableNames, err = s.getDatabaseTables(ctx, req, userName, 0, 0)
	if err != nil {
		return nil, err
	}

	_, err = s.getMiscDatabaseDetails(ctx, req, userName, &resp)
	if err != nil {
		return nil, err
	}

	if req.IncludeStats {
		tableSpans, err := s.getDatabaseTableSpans(ctx, userName, req.Database, resp.TableNames)
		if err != nil {
			return nil, err
		}
		resp.Stats, err = s.getDatabaseStats(ctx, tableSpans)
		if err != nil {
			return nil, err
		}
		dbIndexRecommendations, err := getDatabaseIndexRecommendations(
			ctx, req.Database, s.internalExecutor, s.st, s.sqlServer.execCfg.UnusedIndexRecommendationsKnobs,
		)
		if err != nil {
			return nil, err
		}
		resp.Stats.NumIndexRecommendations = int32(len(dbIndexRecommendations))
	}
	return &resp, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) getDatabaseTableSpans(
	ctx context.Context, userName username.SQLUsername, dbName string, tableNames []string,
) (map[string]roachpb.Span, error) {
	tableSpans := make(map[string]roachpb.Span, len(tableNames))

	for _, tableName := range tableNames {
		fullyQualifiedTableName, err := getFullyQualifiedTableName(dbName, tableName)
		if err != nil {
			return nil, err
		}
		tableID, err := s.queryTableID(ctx, userName, dbName, fullyQualifiedTableName)
		if err != nil {
			return nil, err
		}
		tableSpans[tableName] = generateTableSpan(tableID, s.sqlServer.execCfg.Codec)
	}
	return tableSpans, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) getDatabaseStats(
	ctx context.Context, tableSpans map[string]roachpb.Span,
) (*serverpb.DatabaseDetailsResponse_Stats, error) {
	var stats serverpb.DatabaseDetailsResponse_Stats

	type tableStatsResponse struct {
		name string
		resp *serverpb.TableStatsResponse
		err  error
	}

	// Note that this semaphore at this level is not ideal. Further down we're
	// going to launch goroutines to fetch table stats from each kv node. We
	// cannot use the underlying stats limiter to also limit the number of tables
	// we're currently fetching because it could lead to deadlocks. Instead we
	// create a new limiter here that will lead to at most as many table fetches
	// in flight as there are underlying requests in flight. This is more
	// goroutines than we ought to launch. More ideally we'd have a two-level
	// limiting scheme whereby we have a limit on tables and then a separate
	// limit on stats requests being sent to KV nodes. Such a two-level scheme
	// would provide fairness such that earlier requests finish before later
	// requests.
	sem := quotapool.NewIntPool(
		"database stats", s.statsLimiter.Capacity(),
	)
	responses := make(chan tableStatsResponse, len(tableSpans))
	for tableName, tableSpan := range tableSpans {
		if err := s.sqlServer.stopper.RunAsyncTaskEx(
			ctx, stop.TaskOpts{
				TaskName:   "server.adminServer: requesting table stats",
				Sem:        sem,
				WaitForSem: true,
			},
			func(ctx context.Context) {
				statsResponse, err := s.statsForSpan(ctx, tableSpan)

				responses <- tableStatsResponse{
					name: tableName,
					resp: statsResponse,
					err:  err,
				}
			}); err != nil {
			return nil, err
		}
	}

	// Track all nodes storing databases.
	nodeIDs := make(map[roachpb.NodeID]struct{})
	for i := 0; i < len(tableSpans); i++ {
		select {
		case response := <-responses:
			if response.err != nil {
				stats.MissingTables = append(
					stats.MissingTables,
					&serverpb.DatabaseDetailsResponse_Stats_MissingTable{
						Name:         response.name,
						ErrorMessage: response.err.Error(),
					})
			} else {
				stats.RangeCount += response.resp.RangeCount
				stats.ApproximateDiskBytes += response.resp.ApproximateDiskBytes
				for _, id := range response.resp.NodeIDs {
					nodeIDs[id] = struct{}{}
				}
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	stats.NodeIDs = make([]roachpb.NodeID, 0, len(nodeIDs))
	for id := range nodeIDs {
		stats.NodeIDs = append(stats.NodeIDs, id)
	}
	sort.Slice(stats.NodeIDs, func(i, j int) bool {
		return stats.NodeIDs[i] < stats.NodeIDs[j]
	})

	return &stats, nil
}

// getFullyQualifiedTableName, given a database name and a tableName that either
// is a unqualified name or a schema-qualified name, returns a maximally
// qualified name: either database.table if the input wasn't schema qualified,
// or database.schema.table if it was.
//
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func getFullyQualifiedTableName(dbName string, tableName string) (string, error) {
	name, err := parser.ParseQualifiedTableName(tableName)
	if err != nil {
		// If we got a parse error, it could be that the user passed us an unescaped
		// table name. Quote the whole thing and try again.
		name, err = parser.ParseQualifiedTableName(tree.NameStringP(&tableName))
		if err != nil {
			return "", err
		}
	}
	if !name.ExplicitSchema {
		// If the schema wasn't explicitly set, craft the qualified table name to be
		// database.table.
		name.SchemaName = tree.Name(dbName)
		name.ExplicitSchema = true
	} else {
		// Otherwise, add the database to the beginning of the name:
		// database.schema.table.
		name.CatalogName = tree.Name(dbName)
		name.ExplicitCatalog = true
	}
	return name.String(), nil
}

// TableDetails is an endpoint that returns columns, indices, and other
// relevant details for the specified table.
func (s *adminServer) TableDetails(
	ctx context.Context, req *serverpb.TableDetailsRequest,
) (_ *serverpb.TableDetailsResponse, retErr error) {
	ctx = s.AnnotateCtx(ctx)
	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	if err := s.privilegeChecker.RequireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	r, err := s.tableDetailsHelper(ctx, req, userName)
	return r, maybeHandleNotFoundError(ctx, err)
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) tableDetailsHelper(
	ctx context.Context, req *serverpb.TableDetailsRequest, userName username.SQLUsername,
) (_ *serverpb.TableDetailsResponse, retErr error) {
	escQualTable, err := getFullyQualifiedTableName(req.Database, req.Table)
	if err != nil {
		return nil, err
	}

	var resp serverpb.TableDetailsResponse

	// Marshal SHOW COLUMNS result.
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-columns",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW COLUMNS FROM %s", escQualTable),
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)
	// TODO(cdo): protobuf v3's default behavior for fields with zero values (e.g. empty strings)
	// is to suppress them. So, if protobuf field "foo" is an empty string, "foo" won't show
	// up in the marshaled JSON. I feel that this is counterintuitive, and this should be fixed
	// for our API.
	{
		const (
			colCol     = "column_name"
			typeCol    = "data_type"
			nullCol    = "is_nullable"
			defaultCol = "column_default"
			genCol     = "generation_expression"
			hiddenCol  = "is_hidden"
		)
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			// If ok == false, the query returned 0 rows.
			scanner := makeResultScanner(it.Types())
			for ; ok; ok, err = it.Next(ctx) {
				row := it.Cur()
				var col serverpb.TableDetailsResponse_Column
				if err := scanner.Scan(row, colCol, &col.Name); err != nil {
					return nil, err
				}
				if err := scanner.Scan(row, typeCol, &col.Type); err != nil {
					return nil, err
				}
				if err := scanner.Scan(row, nullCol, &col.Nullable); err != nil {
					return nil, err
				}
				if err := scanner.Scan(row, hiddenCol, &col.Hidden); err != nil {
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
				isGenNull, err := scanner.IsNull(row, genCol)
				if err != nil {
					return nil, err
				}
				if !isGenNull {
					if err := scanner.Scan(row, genCol, &col.GenerationExpression); err != nil {
						return nil, err
					}
				}
				resp.Columns = append(resp.Columns, col)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	// Marshal SHOW INDEX result.
	it, err = s.internalExecutor.QueryIteratorEx(
		ctx, "admin-showindex", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW INDEX FROM %s", escQualTable),
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)
	{
		const (
			nameCol      = "index_name"
			nonUniqueCol = "non_unique"
			seqCol       = "seq_in_index"
			columnCol    = "column_name"
			directionCol = "direction"
			storingCol   = "storing"
			implicitCol  = "implicit"
		)
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			// If ok == false, the query returned 0 rows.
			scanner := makeResultScanner(it.Types())
			for ; ok; ok, err = it.Next(ctx) {
				row := it.Cur()
				// Marshal grant, splitting comma-separated privileges into a proper slice.
				var index serverpb.TableDetailsResponse_Index
				if err := scanner.Scan(row, nameCol, &index.Name); err != nil {
					return nil, err
				}
				var nonUnique bool
				if err := scanner.Scan(row, nonUniqueCol, &nonUnique); err != nil {
					return nil, err
				}
				index.Unique = !nonUnique
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
				if err := scanner.Scan(row, implicitCol, &index.Implicit); err != nil {
					return nil, err
				}
				resp.Indexes = append(resp.Indexes, index)
			}
			if err != nil {
				return nil, err
			}
		}
	}

	// Marshal SHOW GRANTS result.
	it, err = s.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-grants", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW GRANTS ON TABLE %s", escQualTable),
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)
	{
		const (
			userCol       = "grantee"
			privilegesCol = "privilege_type"
		)
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			// If ok == false, the query returned 0 rows.
			scanner := makeResultScanner(it.Types())
			for ; ok; ok, err = it.Next(ctx) {
				row := it.Cur()
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
			if err != nil {
				return nil, err
			}
		}
	}

	// Marshal SHOW CREATE result.
	row, cols, err := s.internalExecutor.QueryRowExWithCols(
		ctx, "admin-show-create", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW CREATE %s", escQualTable),
	)
	if err != nil {
		return nil, err
	}
	{
		const createCol = "create_statement"
		if row == nil {
			return nil, errors.New("create response not available")
		}

		scanner := makeResultScanner(cols)
		var createStmt string
		if err := scanner.Scan(row, createCol, &createStmt); err != nil {
			return nil, err
		}

		resp.CreateTableStatement = createStmt
	}

	// MVCC Garbage result.
	row, cols, err = s.internalExecutor.QueryRowExWithCols(
		ctx, "admin-show-mvcc-garbage-info", nil,
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf(
			`WITH
			range_stats AS (
				SELECT crdb_internal.range_stats(raw_start_key) AS d
				FROM [SHOW RANGES FROM TABLE %s WITH KEYS]
			),
			aggregated AS (
				SELECT
					sum((d->>'live_bytes')::INT8) AS live,
					sum(
						(d->>'key_bytes')::INT8 +
						(d->>'val_bytes')::INT8 +
						COALESCE((d->>'range_key_bytes')::INT8, 0) +
						COALESCE((d->>'range_val_bytes')::INT8, 0) +
						(d->>'sys_bytes')::INT8) AS total
				FROM
					range_stats
			)
			SELECT
				COALESCE(total, 0)::INT8 as total_bytes,
				COALESCE(live, 0)::INT8 as live_bytes,
				COALESCE(live / NULLIF(total,0), 0)::FLOAT8 as live_percentage
			FROM aggregated`,
			escQualTable),
	)
	if err != nil {
		return nil, err
	}
	if row != nil {
		scanner := makeResultScanner(cols)
		var totalBytes int64
		if err := scanner.Scan(row, "total_bytes", &totalBytes); err != nil {
			return nil, err
		}
		resp.DataTotalBytes = totalBytes

		var liveBytes int64
		if err := scanner.Scan(row, "live_bytes", &liveBytes); err != nil {
			return nil, err
		}
		resp.DataLiveBytes = liveBytes

		var livePct float32
		if err := scanner.Scan(row, "live_percentage", &livePct); err != nil {
			return nil, err
		}
		resp.DataLivePercentage = livePct
	}

	// Marshal SHOW STATISTICS result.
	row, cols, err = s.internalExecutor.QueryRowExWithCols(
		ctx, "admin-show-statistics", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SELECT max(created) AS stats_last_created_at FROM [SHOW STATISTICS FOR TABLE %s]", escQualTable),
	)
	if err != nil {
		return nil, err
	}
	if row != nil {
		scanner := makeResultScanner(cols)
		const createdCol = "stats_last_created_at"
		var createdTs *time.Time
		if err := scanner.Scan(row, createdCol, &createdTs); err != nil {
			return nil, err
		}
		resp.StatsLastCreatedAt = createdTs
	}

	// Marshal SHOW ZONE CONFIGURATION result.
	row, cols, err = s.internalExecutor.QueryRowExWithCols(
		ctx, "admin-show-zone-config", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW ZONE CONFIGURATION FOR TABLE %s", escQualTable))
	if err != nil {
		return nil, err
	}
	{
		const rawConfigSQLColName = "raw_config_sql"
		if row == nil {
			return nil, errors.New("show zone config response not available")
		}

		scanner := makeResultScanner(cols)
		var configureZoneStmt string
		if err := scanner.Scan(row, rawConfigSQLColName, &configureZoneStmt); err != nil {
			return nil, err
		}
		resp.ConfigureZoneStatement = configureZoneStmt
	}

	var tableID descpb.ID
	// Query the descriptor ID and zone configuration for this table.
	{
		databaseID, err := s.queryDatabaseID(ctx, userName, req.Database)
		if err != nil {
			return nil, err
		}
		tableID, err = s.queryTableID(ctx, userName, req.Database, escQualTable)
		if err != nil {
			return nil, err
		}
		resp.DescriptorID = int64(tableID)

		id, zone, zoneExists, err := s.queryZonePath(ctx, userName, []descpb.ID{databaseID, tableID})
		if err != nil {
			return nil, err
		}

		if !zoneExists {
			zone = s.sqlServer.cfg.DefaultZoneConfig
		}
		resp.ZoneConfig = zone

		switch id {
		case databaseID:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_DATABASE
		case tableID:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_TABLE
		default:
			resp.ZoneConfigLevel = serverpb.ZoneConfigurationLevel_CLUSTER
		}
	}

	// Get the number of ranges in the table. We get the key span for the table
	// data. Then, we count the number of ranges that make up that key span.
	{
		tableSpan := generateTableSpan(tableID, s.sqlServer.execCfg.Codec)
		tableRSpan, err := keys.SpanAddr(tableSpan)
		if err != nil {
			return nil, err
		}
		rangeCount, err := s.distSender.CountRanges(ctx, tableRSpan)
		if err != nil {
			return nil, err
		}
		resp.RangeCount = rangeCount
	}

	idxUsageStatsProvider := s.sqlServer.pgServer.SQLServer.GetLocalIndexStatistics()
	tableIndexStatsRequest := &serverpb.TableIndexStatsRequest{
		Database: req.Database,
		Table:    req.Table,
	}
	tableIndexStatsResponse, err := getTableIndexUsageStats(ctx,
		tableIndexStatsRequest,
		idxUsageStatsProvider,
		s.internalExecutor,
		s.st,
		s.sqlServer.execCfg)
	if err != nil {
		return nil, err
	}
	resp.HasIndexRecommendations = len(tableIndexStatsResponse.IndexRecommendations) > 0
	return &resp, nil
}

// generateTableSpan generates a table's key span.
//
// NOTE: this doesn't make sense for interleaved (children) table. As of
// 03/2018, callers around here use it anyway.
func generateTableSpan(tableID descpb.ID, codec keys.SQLCodec) roachpb.Span {
	tableStartKey := codec.TablePrefix(uint32(tableID))
	tableEndKey := tableStartKey.PrefixEnd()
	return roachpb.Span{Key: tableStartKey, EndKey: tableEndKey}
}

// TableStats is an endpoint that returns disk usage and replication statistics
// for the specified table.
func (s *adminServer) TableStats(
	ctx context.Context, req *serverpb.TableStatsRequest,
) (*serverpb.TableStatsResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	err = s.privilegeChecker.RequireViewActivityPermission(ctx)
	if err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	escQualTable, err := getFullyQualifiedTableName(req.Database, req.Table)
	if err != nil {
		return nil, maybeHandleNotFoundError(ctx, err)
	}

	tableID, err := s.queryTableID(ctx, userName, req.Database, escQualTable)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	tableSpan := generateTableSpan(tableID, s.sqlServer.execCfg.Codec)

	r, err := s.statsForSpan(ctx, tableSpan)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return r, nil
}

// NonTableStats is an endpoint that returns disk usage and replication
// statistics for non-table parts of the system.
func (s *adminServer) NonTableStats(
	ctx context.Context, req *serverpb.NonTableStatsRequest,
) (*serverpb.NonTableStatsResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	if err := s.privilegeChecker.RequireViewActivityPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	timeSeriesStats, err := s.statsForSpan(ctx, roachpb.Span{
		Key:    keys.TimeseriesPrefix,
		EndKey: keys.TimeseriesPrefix.PrefixEnd(),
	})
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	response := serverpb.NonTableStatsResponse{
		TimeSeriesStats: timeSeriesStats,
	}

	spansForInternalUse := []roachpb.Span{
		{
			Key:    keys.LocalMax,
			EndKey: keys.TimeseriesPrefix,
		},
		{
			Key:    keys.TimeseriesKeyMax,
			EndKey: keys.TableDataMin,
		},
		{
			Key:    keys.TableDataMin,
			EndKey: keys.SystemDescriptorTableSpan.Key,
		},
	}
	for _, span := range spansForInternalUse {
		nonTableStats, err := s.statsForSpan(ctx, span)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		if response.InternalUseStats == nil {
			response.InternalUseStats = nonTableStats
		} else {
			response.InternalUseStats.Add(nonTableStats)
		}
	}

	// There are six empty ranges for table descriptors 17, 18, 19, 22, 29, and
	// 37 that aren't actually tables (a.k.a. the PseudoTableIDs in pkg/keys).
	// No data is ever really written to them since they don't have actual
	// tables. Some backend work could probably be done to eliminate these empty
	// ranges, but it may be more trouble than it's worth. In the meantime,
	// sweeping them under the general-purpose "Internal use" label in
	// the "Non-Table" section of the Databases page.
	response.InternalUseStats.RangeCount += nonTableDescriptorRangeCount()

	return &response, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
//
// TODO(clust-obs): This method should not be implemented on top of
// `adminServer`. There should be a better place for it.
func (s *adminServer) statsForSpan(
	ctx context.Context, span roachpb.Span,
) (*serverpb.TableStatsResponse, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Get a list of nodeIDs, range counts, and replica counts per node
	// for the specified span.
	nodeIDs, rangeCount, replCounts, err := s.getSpanDetails(ctx, span)
	if err != nil {
		return nil, err
	}

	// Construct TableStatsResponse by sending an RPC to every node involved.
	tableStatResponse := serverpb.TableStatsResponse{
		NodeCount: int64(len(nodeIDs)),
		NodeIDs:   nodeIDs,
		// TODO(mrtracy): The "RangeCount" returned by TableStats is more
		// accurate than the "RangeCount" returned by TableDetails, because this
		// method always consistently queries the meta2 key range for the table;
		// in contrast, TableDetails uses a method on the DistSender, which
		// queries using a range metadata cache and thus may return stale data
		// for tables that are rapidly splitting. However, one potential
		// *advantage* of using the DistSender is that it will populate the
		// DistSender's range metadata cache in the case where meta2 information
		// for this table is not already present; the query used by TableStats
		// does not populate the DistSender cache. We should consider plumbing
		// TableStats' meta2 query through the DistSender so that it will share
		// the advantage of populating the cache (without the disadvantage of
		// potentially returning stale data).
		// See GitHub #5435 for some discussion.
		RangeCount: rangeCount,
	}
	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *roachpb.SpanStatsResponse
		err    error
	}

	// Send a SpanStats query to each node.
	responses := make(chan nodeResponse, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		nodeID := nodeID // avoid data race
		if err := s.sqlServer.stopper.RunAsyncTaskEx(
			ctx, stop.TaskOpts{
				TaskName:   "server.adminServer: requesting remote stats",
				Sem:        s.statsLimiter,
				WaitForSem: true,
			},
			func(ctx context.Context) {
				// Set a generous timeout on the context for each individual query.
				var spanResponse *roachpb.SpanStatsResponse
				err := timeutil.RunWithTimeout(ctx, "request remote stats", 20*time.Second,
					func(ctx context.Context) error {
						conn, err := s.serverIterator.dialNode(ctx, serverID(nodeID))
						if err == nil {
							client := serverpb.NewStatusClient(conn)
							req := roachpb.SpanStatsRequest{
								Spans:  []roachpb.Span{span},
								NodeID: nodeID.String(),
							}
							spanResponse, err = client.SpanStats(ctx, &req)
						}
						return err
					})

				// Channel is buffered, can always write.
				responses <- nodeResponse{
					nodeID: nodeID,
					resp:   spanResponse,
					err:    err,
				}
			}); err != nil {
			return nil, err
		}
	}

	// The semantics of tableStatResponse.ReplicaCount counts replicas
	// found for this span returned by a cluster-wide fan-out.
	// We can use descriptors to know what the final count _should_ be,
	// if we assume every request succeeds (nodes and replicas are reachable).
	for _, replCount := range replCounts {
		tableStatResponse.ReplicaCount += replCount
	}

	for remainingResponses := len(nodeIDs); remainingResponses > 0; remainingResponses-- {
		select {
		case resp := <-responses:
			// For nodes which returned an error, note that the node's data
			// is missing. For successful calls, aggregate statistics.
			if resp.err != nil {
				if s, ok := grpcstatus.FromError(errors.UnwrapAll(resp.err)); ok && s.Code() == codes.PermissionDenied {
					return nil, srverrors.ServerError(ctx, resp.err)
				}

				// If this node is unreachable,
				// it's replicas can not be counted.
				tableStatResponse.ReplicaCount -= replCounts[resp.nodeID]

				tableStatResponse.MissingNodes = append(
					tableStatResponse.MissingNodes,
					serverpb.TableStatsResponse_MissingNode{
						NodeID:       resp.nodeID.String(),
						ErrorMessage: resp.err.Error(),
					},
				)
			} else {
				tableStatResponse.Stats.Add(resp.resp.SpanToStats[span.String()].TotalStats)
				tableStatResponse.ApproximateDiskBytes += resp.resp.SpanToStats[span.String()].ApproximateDiskBytes
			}
		case <-ctx.Done():
			// Caller gave up, stop doing work.
			return nil, ctx.Err()
		}
	}

	return &tableStatResponse, nil
}

// Returns the list of node ids, range count,
// and replica count for the specified span.
func (s *adminServer) getSpanDetails(
	ctx context.Context, span roachpb.Span,
) (nodeIDList []roachpb.NodeID, rangeCount int64, replCounts map[roachpb.NodeID]int64, _ error) {
	nodeIDs := make(map[roachpb.NodeID]struct{})
	replCountForNodeID := make(map[roachpb.NodeID]int64)
	var it rangedesc.Iterator
	var err error
	if s.sqlServer.tenantConnect == nil {
		it, err = s.sqlServer.execCfg.RangeDescIteratorFactory.NewIterator(ctx, span)
	} else {
		it, err = s.sqlServer.tenantConnect.NewIterator(ctx, span)
	}
	if err != nil {
		return nil, 0, nil, err
	}
	var rangeDesc roachpb.RangeDescriptor
	for ; it.Valid(); it.Next() {
		rangeCount++
		rangeDesc = it.CurRangeDescriptor()
		for _, repl := range rangeDesc.Replicas().Descriptors() {
			replCountForNodeID[repl.NodeID]++
			nodeIDs[repl.NodeID] = struct{}{}
		}
	}

	nodeIDList = make([]roachpb.NodeID, 0, len(nodeIDs))
	for id := range nodeIDs {
		nodeIDList = append(nodeIDList, id)
	}
	sort.Slice(nodeIDList, func(i, j int) bool {
		return nodeIDList[i] < nodeIDList[j]
	})
	return nodeIDList, rangeCount, replCountForNodeID, nil
}

// Users returns a list of users, stripped of any passwords.
func (s *adminServer) Users(
	ctx context.Context, req *serverpb.UsersRequest,
) (_ *serverpb.UsersResponse, retErr error) {
	ctx = s.AnnotateCtx(ctx)
	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	r, err := s.usersHelper(ctx, req, userName)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return r, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) usersHelper(
	ctx context.Context, req *serverpb.UsersRequest, userName username.SQLUsername,
) (_ *serverpb.UsersResponse, retErr error) {
	query := `SELECT username FROM system.users WHERE "isRole" = false`
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-users", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		query,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator.
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	var resp serverpb.UsersResponse
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		resp.Users = append(resp.Users, serverpb.UsersResponse_User{Username: string(tree.MustBeDString(row[0]))})
	}
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

var eventSetClusterSettingName = logpb.GetEventTypeName(&eventpb.SetClusterSetting{})

// combineAllErrors combines all passed-in errors into a single object.
func combineAllErrors(errs []error) error {
	var combinedErrors error
	for _, err := range errs {
		combinedErrors = errors.CombineErrors(combinedErrors, err)
	}
	return combinedErrors
}

// Events is an endpoint that returns the latest event log entries, with the following
// optional URL parameters:
//
// type=STRING  returns events with this type (e.g. "create_table")
// targetID=INT returns events for that have this targetID
func (s *adminServer) Events(
	ctx context.Context, req *serverpb.EventsRequest,
) (_ *serverpb.EventsResponse, retErr error) {
	ctx = s.AnnotateCtx(ctx)

	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	redactEvents := !req.UnredactedEvents

	limit := req.Limit
	if limit == 0 {
		limit = apiconstants.DefaultAPIEventLimit
	}

	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	r, err := s.eventsHelper(ctx, req, userName, int(limit), 0, redactEvents)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return r, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) eventsHelper(
	ctx context.Context,
	req *serverpb.EventsRequest,
	userName username.SQLUsername,
	limit, offset int,
	redactEvents bool,
) (_ *serverpb.EventsResponse, retErr error) {
	// Execute the query.
	q := safesql.NewQuery()
	q.Append(`SELECT timestamp, "eventType", "reportingID", info, "uniqueID" `)
	q.Append("FROM system.eventlog ")
	q.Append("WHERE true ") // This simplifies the WHERE clause logic below.
	if len(req.Type) > 0 {
		q.Append(`AND "eventType" = $ `, req.Type)
	}
	q.Append("ORDER BY timestamp DESC ")
	if limit > 0 {
		q.Append("LIMIT $", limit)
		if offset > 0 {
			q.Append(" OFFSET $", offset)
		}
	}
	if len(q.Errors()) > 0 {
		return nil, combineAllErrors(q.Errors())
	}
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-events", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		q.String(), q.QueryArguments()...)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	// Marshal response.
	var resp serverpb.EventsResponse
	ok, err := it.Next(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		// The query returned 0 rows.
		return &resp, nil
	}
	scanner := makeResultScanner(it.Types())
	for ; ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		var event serverpb.EventsResponse_Event
		var ts time.Time
		if err := scanner.ScanIndex(row, 0, &ts); err != nil {
			return nil, err
		}
		event.Timestamp = ts
		if err := scanner.ScanIndex(row, 1, &event.EventType); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 2, &event.ReportingID); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 3, &event.Info); err != nil {
			return nil, err
		}
		if event.EventType == eventSetClusterSettingName {
			if redactEvents {
				event.Info = redactSettingsChange(event.Info)
			}
		}
		if err := scanner.ScanIndex(row, 4, &event.UniqueID); err != nil {
			return nil, err
		}
		if redactEvents {
			event.Info = redactStatement(event.Info)
		}

		resp.Events = append(resp.Events, event)
	}
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// make a best-effort attempt at redacting the setting value.
func redactSettingsChange(info string) string {
	var s eventpb.SetClusterSetting
	if err := json.Unmarshal([]byte(info), &s); err != nil {
		return ""
	}
	s.Value = "<hidden>"
	ret, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(ret)
}

// make a best-effort attempt at redacting the statement details.
func redactStatement(info string) string {
	s := map[string]interface{}{}
	if err := json.Unmarshal([]byte(info), &s); err != nil {
		return info
	}
	if _, ok := s["Statement"]; ok {
		s["Statement"] = "<hidden>"
	}
	ret, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(ret)
}

// RangeLog is an endpoint that returns the latest range log entries.
func (s *adminServer) RangeLog(
	ctx context.Context, req *serverpb.RangeLogRequest,
) (_ *serverpb.RangeLogResponse, retErr error) {
	ctx = s.AnnotateCtx(ctx)

	// Range keys, even when pretty-printed, contain PII.
	user, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, err
	}

	err = s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	r, err := s.rangeLogHelper(ctx, req, user)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return r, nil
}

func (s *adminServer) rangeLogHelper(
	ctx context.Context, req *serverpb.RangeLogRequest, userName username.SQLUsername,
) (_ *serverpb.RangeLogResponse, retErr error) {
	limit := req.Limit
	if limit == 0 {
		limit = apiconstants.DefaultAPIEventLimit
	}

	// Execute the query.
	q := safesql.NewQuery()
	q.Append(`SELECT timestamp, "rangeID", "storeID", "eventType", "otherRangeID", info `)
	q.Append("FROM system.rangelog ")
	if req.RangeId > 0 {
		rangeID := tree.NewDInt(tree.DInt(req.RangeId))
		q.Append(`WHERE "rangeID" = $ OR "otherRangeID" = $`, rangeID, rangeID)
	}
	if limit > 0 {
		q.Append("ORDER BY timestamp desc ")
		q.Append("LIMIT $", tree.NewDInt(tree.DInt(limit)))
	}
	if len(q.Errors()) > 0 {
		return nil, combineAllErrors(q.Errors())
	}
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-range-log", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		q.String(), q.QueryArguments()...,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	// Marshal response.
	var resp serverpb.RangeLogResponse
	ok, err := it.Next(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		// The query returned 0 rows.
		return &resp, nil
	}
	cols := it.Types()
	if len(cols) != 6 {
		return nil, errors.Errorf("incorrect number of columns in response, expected 6, got %d", len(cols))
	}
	scanner := makeResultScanner(cols)
	for ; ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		var event kvserverpb.RangeLogEvent
		var ts time.Time
		if err := scanner.ScanIndex(row, 0, &ts); err != nil {
			return nil, errors.Wrapf(err, "timestamp didn't parse correctly: %s", row[0].String())
		}
		event.Timestamp = ts
		var rangeID int64
		if err := scanner.ScanIndex(row, 1, &rangeID); err != nil {
			return nil, errors.Wrapf(err, "RangeID didn't parse correctly: %s", row[1].String())
		}
		event.RangeID = roachpb.RangeID(rangeID)
		var storeID int64
		if err := scanner.ScanIndex(row, 2, &storeID); err != nil {
			return nil, errors.Wrapf(err, "StoreID didn't parse correctly: %s", row[2].String())
		}
		event.StoreID = roachpb.StoreID(int32(storeID))
		var eventTypeString string
		if err := scanner.ScanIndex(row, 3, &eventTypeString); err != nil {
			return nil, errors.Wrapf(err, "EventType didn't parse correctly: %s", row[3].String())
		}
		if eventType, ok := kvserverpb.RangeLogEventType_value[eventTypeString]; ok {
			event.EventType = kvserverpb.RangeLogEventType(eventType)
		} else {
			return nil, errors.Errorf("EventType didn't parse correctly: %s", eventTypeString)
		}

		var otherRangeID int64
		if row[4].String() != "NULL" {
			if err := scanner.ScanIndex(row, 4, &otherRangeID); err != nil {
				return nil, errors.Wrapf(err, "OtherRangeID didn't parse correctly: %s", row[4].String())
			}
			event.OtherRangeID = roachpb.RangeID(otherRangeID)
		}

		var prettyInfo serverpb.RangeLogResponse_PrettyInfo
		if row[5].String() != "NULL" {
			var info string
			if err := scanner.ScanIndex(row, 5, &info); err != nil {
				return nil, errors.Wrapf(err, "info didn't parse correctly: %s", row[5].String())
			}
			if err := json.Unmarshal([]byte(info), &event.Info); err != nil {
				return nil, errors.Wrapf(err, "info didn't parse correctly: %s", info)
			}
			if event.Info.NewDesc != nil {
				prettyInfo.NewDesc = event.Info.NewDesc.String()
			}
			if event.Info.UpdatedDesc != nil {
				prettyInfo.UpdatedDesc = event.Info.UpdatedDesc.String()
			}
			if event.Info.AddedReplica != nil {
				prettyInfo.AddedReplica = event.Info.AddedReplica.String()
			}
			if event.Info.RemovedReplica != nil {
				prettyInfo.RemovedReplica = event.Info.RemovedReplica.String()
			}
			prettyInfo.Reason = string(event.Info.Reason)
			prettyInfo.Details = event.Info.Details
		}

		resp.Events = append(resp.Events, serverpb.RangeLogResponse_Event{
			Event:      event,
			PrettyInfo: prettyInfo,
		})
	}
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// getUIData returns the values and timestamps for the given UI keys. Keys
// that are not found will not be returned.
//
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) getUIData(
	ctx context.Context, userName username.SQLUsername, keys []string,
) (_ *serverpb.GetUIDataResponse, retErr error) {
	if len(keys) == 0 {
		return &serverpb.GetUIDataResponse{}, nil
	}

	// Query database.
	query := safesql.NewQuery()
	query.Append(`SELECT key, value, "lastUpdated" FROM system.ui WHERE key IN (`)
	for i, key := range keys {
		if i != 0 {
			query.Append(",")
		}
		query.Append("$", tree.NewDString(makeUIKey(userName, key)))
	}
	query.Append(");")
	if errs := query.Errors(); len(errs) > 0 {
		var err error
		for _, e := range errs {
			err = errors.CombineErrors(err, e)
		}
		return nil, errors.Wrap(err, "error constructing query")
	}
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-getUIData", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		query.String(), query.QueryArguments()...,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	// Marshal results.
	resp := serverpb.GetUIDataResponse{KeyValues: make(map[string]serverpb.GetUIDataResponse_Value)}
	var hasNext bool
	for hasNext, err = it.Next(ctx); hasNext; hasNext, err = it.Next(ctx) {
		row := it.Cur()
		dKey, ok := tree.AsDString(row[0])
		if !ok {
			return nil, errors.Errorf("unexpected type for UI key: %T", row[0])
		}
		_, key := splitUIKey(string(dKey))
		dKey = tree.DString(key)

		dValue, ok := row[1].(*tree.DBytes)
		if !ok {
			return nil, errors.Errorf("unexpected type for UI value: %T", row[1])
		}
		dLastUpdated, ok := row[2].(*tree.DTimestamp)
		if !ok {
			return nil, errors.Errorf("unexpected type for UI lastUpdated: %T", row[2])
		}

		resp.KeyValues[string(dKey)] = serverpb.GetUIDataResponse_Value{
			Value:       []byte(*dValue),
			LastUpdated: dLastUpdated.Time,
		}
	}
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// makeUIKey combines username and key to form a lookup key in
// system.ui.
// The username is combined to ensure that different users
// can use different customizations.
func makeUIKey(user username.SQLUsername, key string) string {
	return user.Normalized() + "$" + key
}

// splitUIKey is the inverse of makeUIKey.
// The caller must ensure that the value was produced by makeUIKey.
func splitUIKey(combined string) (string, string) {
	pair := strings.SplitN(combined, "$", 2)
	return pair[0], pair[1]
}

// SetUIData is an endpoint that stores the given key/value pairs in the
// system.ui table. See GetUIData for more details on semantics.
func (s *adminServer) SetUIData(
	ctx context.Context, req *serverpb.SetUIDataRequest,
) (*serverpb.SetUIDataResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	if len(req.KeyValues) == 0 {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "KeyValues cannot be empty")
	}

	for key, val := range req.KeyValues {
		// Do an upsert of the key. We update each key in a separate transaction to
		// avoid long-running transactions and possible deadlocks.
		ie := s.sqlServer.internalDB.Executor()
		query := `UPSERT INTO system.ui (key, value, "lastUpdated") VALUES ($1, $2, now())`
		rowsAffected, err := ie.ExecEx(
			ctx, "admin-set-ui-data", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			query, makeUIKey(userName, key), val)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		if rowsAffected != 1 {
			return nil, srverrors.ServerErrorf(ctx, "rows affected %d != expected %d", rowsAffected, 1)
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
func (s *adminServer) GetUIData(
	ctx context.Context, req *serverpb.GetUIDataRequest,
) (*serverpb.GetUIDataResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	if len(req.Keys) == 0 {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "keys cannot be empty")
	}

	resp, err := s.getUIData(ctx, userName, req.Keys)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	return resp, nil
}

// Settings returns settings associated with the given keys.
func (s *adminServer) Settings(
	ctx context.Context, req *serverpb.SettingsRequest,
) (*serverpb.SettingsResponse, error) {
	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	keyFilter := make(map[string]bool)
	for _, key := range req.Keys {
		keyFilter[key] = true
	}
	// Read the system.settings table to determine the settings for which we have
	// explicitly set values -- the in-memory SV has the set and default values
	// flattened for quick reads, but we'd only need the non-defaults for comparison.
	alteredSettings := make(map[settings.InternalKey]*time.Time)
	if it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "read-setting", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT name, "lastUpdated" FROM system.settings`,
	); err != nil {
		log.Warningf(ctx, "failed to read settings: %s", err)
	} else {
		var ok bool
		for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
			row := it.Cur()
			key := settings.InternalKey(tree.MustBeDString(row[0]))
			lastUpdated := row[1].(*tree.DTimestamp)
			alteredSettings[key] = &lastUpdated.Time
		}
		if err != nil {
			// No need to clear AlteredSettings map since we only make best
			// effort to populate it.
			log.Warningf(ctx, "failed to read settings: %s", err)
		}
	}

	// Get cluster settings
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "get-cluster-settings", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		"SELECT variable, value, type, description, public from crdb_internal.cluster_settings",
	)

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	scanner := makeResultScanner(it.Types())
	resp := serverpb.SettingsResponse{KeyValues: make(map[string]serverpb.SettingsResponse_Value)}
	respSettings := make(map[string]serverpb.SettingsResponse_Value)
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		var responseValue serverpb.SettingsResponse_Value
		if scanErr := scanner.ScanAll(
			row,
			&responseValue.Name,
			&responseValue.Value,
			&responseValue.Type,
			&responseValue.Description,
			&responseValue.Public); scanErr != nil {
			return nil, srverrors.ServerError(ctx, scanErr)
		}
		internalKey, found, _ := settings.NameToKey(settings.SettingName(responseValue.Name))

		if found && (len(keyFilter) == 0 || keyFilter[string(internalKey)]) {
			if lastUpdated, found := alteredSettings[internalKey]; found {
				responseValue.LastUpdated = lastUpdated
			}
			respSettings[string(internalKey)] = responseValue
		}
	}

	// Users without MODIFYCLUSTERSETTINGS or VIEWCLUSTERSETTINGS access cannot see the values.
	// Exception: users with VIEWACTIVITY and VIEWACTIVITYREDACTED can see cluster
	// settings used by the UI Console.
	if err != nil {
		if pgerror.GetPGCode(err) != pgcode.InsufficientPrivilege {
			return nil, srverrors.ServerError(ctx, err)
		}
		if err2 := s.privilegeChecker.RequireViewActivityOrViewActivityRedactedPermission(ctx); err2 != nil {
			// The check for VIEWACTIVITY or VIEWATIVITYREDACTED is a special case so cluster settings from
			// the console can be returned, but if the user doesn't have them (i.e. err2 != nil), we don't want
			// to share this error message.
			return nil, grpcstatus.Errorf(
				codes.PermissionDenied, "this operation requires the %s or %s system privileges",
				privilege.VIEWCLUSTERSETTING.DisplayName(), privilege.MODIFYCLUSTERSETTING.DisplayName())
		}
		consoleKeys := settings.ConsoleKeys()
		for _, k := range consoleKeys {
			if consoleSetting, ok := settings.LookupForLocalAccessByKey(k, s.sqlServer.execCfg.Codec.ForSystemTenant()); ok {
				if internalKey, found, _ := settings.NameToKey(consoleSetting.Name()); found &&
					(len(keyFilter) == 0 || keyFilter[string(internalKey)]) {
					var responseValue serverpb.SettingsResponse_Value
					responseValue.Name = string(consoleSetting.Name())
					responseValue.Value = consoleSetting.String(&s.st.SV)
					responseValue.Type = consoleSetting.Typ()
					responseValue.Description = consoleSetting.Description()
					responseValue.Public = consoleSetting.Visibility() == settings.Public
					if lastUpdated, found := alteredSettings[internalKey]; found {
						responseValue.LastUpdated = lastUpdated
					}
					respSettings[string(internalKey)] = responseValue
				}
			}
		}

	}

	resp.KeyValues = respSettings
	return &resp, nil
}

// Cluster returns cluster metadata.
func (s *adminServer) Cluster(
	_ context.Context, req *serverpb.ClusterRequest,
) (*serverpb.ClusterResponse, error) {
	storageClusterID := s.rpcContext.StorageClusterID.Get()
	if storageClusterID == (uuid.UUID{}) {
		return nil, grpcstatus.Errorf(codes.Unavailable, "cluster ID not yet available")
	}

	// Check if enterprise features are enabled.  We currently test for the
	// feature "BACKUP", although enterprise licenses do not yet distinguish
	// between different features.
	enterpriseEnabled := base.CheckEnterpriseEnabled(
		s.st,
		"BACKUP") == nil

	return &serverpb.ClusterResponse{
		// TODO(knz): Respond with the logical cluster ID as well.
		ClusterID:         storageClusterID.String(),
		ReportingEnabled:  logcrash.DiagnosticsReportingEnabled.Get(&s.st.SV),
		EnterpriseEnabled: enterpriseEnabled,
	}, nil
}

// Health returns whether this tenant server is ready to receive
// traffic.
//
// See the docstring for HealthRequest for more details about
// what this function precisely reports.
//
// Note: Health is non-privileged and non-authenticated and thus
// must not report privileged information.
func (s *adminServer) Health(
	ctx context.Context, req *serverpb.HealthRequest,
) (*serverpb.HealthResponse, error) {
	telemetry.Inc(telemetryHealthCheck)

	resp := &serverpb.HealthResponse{}
	// If Ready is not set, the client doesn't want to know whether this node is
	// ready to receive client traffic.
	if !req.Ready {
		return resp, nil
	}

	if err := s.checkReadinessForHealthCheck(ctx); err != nil {
		return nil, err
	}

	return resp, nil
}

// checkReadinessForHealthCheck returns a gRPC error.
func (s *adminServer) checkReadinessForHealthCheck(ctx context.Context) error {
	if err := s.grpc.health(ctx); err != nil {
		return err
	}

	if !s.sqlServer.isReady.Load() {
		return grpcstatus.Errorf(codes.Unavailable, "node is not accepting SQL clients")
	}

	return nil
}

// Health returns liveness for the node target of the request.
//
// See the docstring for HealthRequest for more details about
// what this function precisely reports.
//
// Note: Health is non-privileged and non-authenticated and thus
// must not report privileged information.
func (s *systemAdminServer) Health(
	ctx context.Context, req *serverpb.HealthRequest,
) (*serverpb.HealthResponse, error) {
	telemetry.Inc(telemetryHealthCheck)

	resp := &serverpb.HealthResponse{}
	// If Ready is not set, the client doesn't want to know whether this node is
	// ready to receive client traffic.
	if !req.Ready {
		return resp, nil
	}

	if err := s.checkReadinessForHealthCheck(ctx); err != nil {
		return nil, err
	}
	return resp, nil
}

// checkReadinessForHealthCheck returns a gRPC error.
func (s *systemAdminServer) checkReadinessForHealthCheck(ctx context.Context) error {
	if err := s.grpc.health(ctx); err != nil {
		return err
	}

	status := s.nodeLiveness.GetNodeVitalityFromCache(roachpb.NodeID(s.serverIterator.getID()))
	if !status.IsLive(livenesspb.AdminHealthCheck) {
		return grpcstatus.Errorf(codes.Unavailable, "node is not healthy")
	}

	if !s.sqlServer.isReady.Load() {
		return grpcstatus.Errorf(codes.Unavailable, "node is not accepting SQL clients")
	}

	return nil
}

// getLivenessResponse returns LivenessResponse: a map from NodeID to LivenessStatus and
// a slice containing the liveness record of all nodes that have ever been a part of the
// cluster.
func getLivenessResponse(
	ctx context.Context, nl livenesspb.NodeVitalityInterface,
) (*serverpb.LivenessResponse, error) {
	nodeVitalityMap, err := nl.ScanNodeVitalityFromKV(ctx)

	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	livenesses := make([]livenesspb.Liveness, 0, len(nodeVitalityMap))
	statusMap := make(map[roachpb.NodeID]livenesspb.NodeLivenessStatus, len(nodeVitalityMap))

	for nodeID, vitality := range nodeVitalityMap {
		livenesses = append(livenesses, vitality.GenLiveness())
		statusMap[nodeID] = vitality.LivenessStatus()
	}
	sort.Slice(livenesses, func(i, j int) bool {
		return livenesses[i].NodeID < livenesses[j].NodeID
	})
	return &serverpb.LivenessResponse{
		Livenesses: livenesses,
		Statuses:   statusMap,
	}, nil
}

// Liveness is implemented on the tenant-facing admin server
// as a request through the tenant connector. Since at the
// time of writing, the request contains no additional SQL
// permission checks, the tenant capability gate is all
// that is required. This is handled by the connector.
func (s *adminServer) Liveness(
	ctx context.Context, req *serverpb.LivenessRequest,
) (*serverpb.LivenessResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	return s.sqlServer.tenantConnect.Liveness(ctx, req)
}

// Liveness returns the liveness state of all nodes on the cluster
// based on a KV transaction. To reach all nodes in the cluster, consider
// using (statusServer).NodesWithLiveness instead.
func (s *systemAdminServer) Liveness(
	ctx context.Context, _ *serverpb.LivenessRequest,
) (*serverpb.LivenessResponse, error) {
	return getLivenessResponse(ctx, s.nodeLiveness)
}

func (s *adminServer) Jobs(
	ctx context.Context, req *serverpb.JobsRequest,
) (_ *serverpb.JobsResponse, retErr error) {
	ctx = s.AnnotateCtx(ctx)

	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	j, err := jobsHelper(
		ctx,
		req,
		userName,
		s.sqlServer,
		s.sqlServer.cfg,
		&s.sqlServer.cfg.Settings.SV,
	)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return j, nil
}

// BuildJobQueryFromRequest builds the SQL query for the given
// JobsRequest. This is exported for testing purposes only.
func BuildJobQueryFromRequest(req *serverpb.JobsRequest) *safesql.Query {
	q := safesql.NewQuery()
	q.Append(`
SELECT
  job_id,
  job_type,
  description,
  statement,
  user_name,
  status,
  running_status,
  created,
  finished,
  modified,
  fraction_completed,
  high_water_timestamp,
  error,
  execution_events::string,
  coordinator_id
FROM crdb_internal.jobs
WHERE true`) // Simplifies filter construction below.
	if req.Status != "" {
		q.Append(" AND status = $", req.Status)
	}
	if req.Type != jobspb.TypeUnspecified {
		q.Append(" AND job_type = $", req.Type.String())
	} else {
		// Don't show automatic jobs in the overview page.
		q.Append(" AND ( job_type NOT IN (")
		for idx, jobType := range jobspb.AutomaticJobTypes {
			if idx != 0 {
				q.Append(", ")
			}
			q.Append("$", jobType.String())
		}
		q.Append(" ) OR job_type IS NULL)")
	}
	q.Append(" ORDER BY created DESC")
	if req.Limit > 0 {
		q.Append(" LIMIT $", tree.DInt(req.Limit))
	}
	return q
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func jobsHelper(
	ctx context.Context,
	req *serverpb.JobsRequest,
	userName username.SQLUsername,
	sqlServer *SQLServer,
	cfg *BaseConfig,
	sv *settings.Values,
) (_ *serverpb.JobsResponse, retErr error) {
	q := BuildJobQueryFromRequest(req)
	it, err := sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-jobs", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		q.String(), q.QueryArguments()...,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	ok, err := it.Next(ctx)
	if err != nil {
		return nil, err
	}

	var resp serverpb.JobsResponse

	now := timeutil.Now()
	if cfg.TestingKnobs.Server != nil &&
		cfg.TestingKnobs.Server.(*TestingKnobs).StubTimeNow != nil {
		now = cfg.TestingKnobs.Server.(*TestingKnobs).StubTimeNow()
	}
	retentionDuration := func() time.Duration {
		if cfg.TestingKnobs.JobsTestingKnobs != nil &&
			cfg.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs).IntervalOverrides.RetentionTime != nil {
			return *cfg.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs).IntervalOverrides.RetentionTime
		}
		return jobs.RetentionTimeSetting.Get(sv)
	}
	resp.EarliestRetainedTime = now.Add(-retentionDuration())

	if !ok {
		// The query returned 0 rows.
		return &resp, nil
	}
	scanner := makeResultScanner(it.Types())
	for ; ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		var job serverpb.JobResponse
		err := scanRowIntoJob(scanner, row, &job)
		if err != nil {
			return nil, err
		}

		resp.Jobs = append(resp.Jobs, job)
	}

	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func scanRowIntoJob(scanner resultScanner, row tree.Datums, job *serverpb.JobResponse) error {
	var fractionCompletedOrNil *float32
	var highwaterOrNil *apd.Decimal
	var runningStatusOrNil *string
	var executionFailuresOrNil *string
	var coordinatorOrNil *int64
	if err := scanner.ScanAll(
		row,
		&job.ID,
		&job.Type,
		&job.Description,
		&job.Statement,
		&job.Username,
		&job.Status,
		&runningStatusOrNil,
		&job.Created,
		&job.Finished,
		&job.Modified,
		&fractionCompletedOrNil,
		&highwaterOrNil,
		&job.Error,
		&executionFailuresOrNil,
		&coordinatorOrNil,
	); err != nil {
		return errors.Wrap(err, "scan")
	}
	if highwaterOrNil != nil {
		highwaterTimestamp, err := hlc.DecimalToHLC(highwaterOrNil)
		if err != nil {
			return errors.Wrap(err, "highwater timestamp had unexpected format")
		}
		goTime := highwaterTimestamp.GoTime()
		job.HighwaterTimestamp = &goTime
		job.HighwaterDecimal = highwaterOrNil.String()
	}
	if fractionCompletedOrNil != nil {
		job.FractionCompleted = *fractionCompletedOrNil
	}
	if runningStatusOrNil != nil {
		job.RunningStatus = *runningStatusOrNil
	}
	if coordinatorOrNil != nil {
		job.CoordinatorID = *coordinatorOrNil
	}
	return nil
}

func (s *adminServer) Job(
	ctx context.Context, request *serverpb.JobRequest,
) (_ *serverpb.JobResponse, retErr error) {
	ctx = s.AnnotateCtx(ctx)

	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	r, err := jobHelper(ctx, request, userName, s.sqlServer)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return r, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func jobHelper(
	ctx context.Context,
	request *serverpb.JobRequest,
	userName username.SQLUsername,
	sqlServer *SQLServer,
) (_ *serverpb.JobResponse, retErr error) {
	const query = `
	        SELECT job_id, job_type, description, statement, user_name, status,
	  						 running_status, created, finished, modified,
	  						 fraction_completed, high_water_timestamp, error, execution_events::string, coordinator_id
	          FROM crdb_internal.jobs
	         WHERE job_id = $1`
	row, cols, err := sqlServer.internalExecutor.QueryRowExWithCols(
		ctx, "admin-job", nil,
		sessiondata.InternalExecutorOverride{User: userName},
		query,
		request.JobId,
	)

	if err != nil {
		return nil, errors.Wrapf(err, "expected to find 1 job with job_id=%d", request.JobId)
	}

	if row == nil {
		return nil, errors.Errorf(
			"could not get job for job_id %d; 0 rows returned", request.JobId,
		)
	}

	scanner := makeResultScanner(cols)

	var job serverpb.JobResponse
	err = scanRowIntoJob(scanner, row, &job)
	if err != nil {
		return nil, err
	}

	// On 25.1+, add any recorded job messages to the response as well.
	if sqlServer.cfg.Settings.Version.IsActive(ctx, clusterversion.V25_1) {
		job.Messages = fetchJobMessages(ctx, job.ID, userName, sqlServer)
	}
	return &job, nil
}

func fetchJobMessages(
	ctx context.Context, jobID int64, user username.SQLUsername, sqlServer *SQLServer,
) (messages []serverpb.JobMessage) {
	const msgQuery = `SELECT kind, written, message FROM system.job_message WHERE job_id = $1 ORDER BY written DESC`
	it, err := sqlServer.internalExecutor.QueryIteratorEx(ctx, "admin-job-messages", nil,
		sessiondata.InternalExecutorOverride{User: user},
		msgQuery,
		jobID,
	)

	if err != nil {
		return []serverpb.JobMessage{{Kind: "error", Timestamp: timeutil.Now(), Message: err.Error()}}
	}

	defer func() {
		if err := it.Close(); err != nil {
			messages = []serverpb.JobMessage{{Kind: "error", Timestamp: timeutil.Now(), Message: err.Error()}}
		}
	}()

	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return []serverpb.JobMessage{{Kind: "error", Timestamp: timeutil.Now(), Message: err.Error()}}
		}
		if !ok {
			break
		}
		row := it.Cur()
		messages = append(messages, serverpb.JobMessage{
			Kind:      string(tree.MustBeDStringOrDNull(row[0])),
			Timestamp: tree.MustBeDTimestampTZ(row[1]).Time,
			Message:   string(tree.MustBeDStringOrDNull(row[2])),
		})
	}
	return messages
}

func (s *adminServer) Locations(
	ctx context.Context, req *serverpb.LocationsRequest,
) (_ *serverpb.LocationsResponse, retErr error) {
	ctx = s.AnnotateCtx(ctx)

	// Require authentication.
	_, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	r, err := s.locationsHelper(ctx, req)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return r, nil
}

func (s *adminServer) locationsHelper(
	ctx context.Context, req *serverpb.LocationsRequest,
) (_ *serverpb.LocationsResponse, retErr error) {
	q := safesql.NewQuery()
	q.Append(`SELECT "localityKey", "localityValue", latitude, longitude FROM system.locations`)
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-locations", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		q.String(),
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	ok, err := it.Next(ctx)
	if err != nil {
		return nil, err
	}

	var resp serverpb.LocationsResponse
	if !ok {
		// The query returned 0 rows.
		return &resp, nil
	}
	scanner := makeResultScanner(it.Types())
	for ; ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		var loc serverpb.LocationsResponse_Location
		lat, lon := new(apd.Decimal), new(apd.Decimal)
		if err := scanner.ScanAll(
			row, &loc.LocalityKey, &loc.LocalityValue, lat, lon); err != nil {
			return nil, err
		}
		if loc.Latitude, err = lat.Float64(); err != nil {
			return nil, err
		}
		if loc.Longitude, err = lon.Float64(); err != nil {
			return nil, err
		}
		resp.Locations = append(resp.Locations, loc)
	}

	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// QueryPlan returns a JSON representation of a distsql physical query
// plan.
func (s *adminServer) QueryPlan(
	ctx context.Context, req *serverpb.QueryPlanRequest,
) (*serverpb.QueryPlanResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	// As long as there's only one query provided it's safe to construct the
	// explain query.
	stmts, err := parser.Parse(req.Query)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	if len(stmts) > 1 {
		return nil, srverrors.ServerErrorf(ctx, "more than one query provided")
	}

	explain := fmt.Sprintf(
		"EXPLAIN (DISTSQL, JSON) %s",
		strings.Trim(req.Query, ";"))
	row, err := s.internalExecutor.QueryRowEx(
		ctx, "admin-query-plan", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		explain,
	)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	if row == nil {
		return nil, srverrors.ServerErrorf(ctx, "failed to query the physical plan")
	}

	dbDatum, ok := tree.AsDString(row[0])
	if !ok {
		return nil, srverrors.ServerErrorf(ctx, "type assertion failed on json: %T", row)
	}

	return &serverpb.QueryPlanResponse{
		DistSQLPhysicalQueryPlan: string(dbDatum),
	}, nil
}

// getStatementBundle retrieves the statement bundle with the given id and
// writes it out as an attachment. Note this function assumes the user has
// permission to access the statement bundle.
func (s *adminServer) getStatementBundle(ctx context.Context, id int64, w http.ResponseWriter) {
	row, err := s.internalExecutor.QueryRowEx(
		ctx, "admin-stmt-bundle", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		"SELECT bundle_chunks FROM system.statement_diagnostics WHERE id=$1 AND bundle_chunks IS NOT NULL",
		id,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if row == nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	// Put together the entire bundle. Ideally we would stream it in chunks,
	// but it's hard to return errors once we start.
	var bundle bytes.Buffer
	chunkIDs := row[0].(*tree.DArray).Array
	for _, chunkID := range chunkIDs {
		chunkRow, err := s.internalExecutor.QueryRowEx(
			ctx, "admin-stmt-bundle", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			"SELECT data FROM system.statement_bundle_chunks WHERE id=$1",
			chunkID,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if chunkRow == nil {
			http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
			return
		}
		data := chunkRow[0].(*tree.DBytes)
		bundle.WriteString(string(*data))
	}

	w.Header().Set(
		"Content-Disposition",
		fmt.Sprintf("attachment; filename=stmt-bundle-%d.zip", id),
	)

	_, _ = io.Copy(w, &bundle)
}

// DecommissionPreCheck runs checks and returns the DecommissionPreCheckResponse
// for the given nodes.
func (s *systemAdminServer) DecommissionPreCheck(
	ctx context.Context, req *serverpb.DecommissionPreCheckRequest,
) (*serverpb.DecommissionPreCheckResponse, error) {
	var collectTraces bool
	if s := tracing.SpanFromContext(ctx); (s != nil && s.RecordingType() != tracingpb.RecordingOff) || req.CollectTraces {
		collectTraces = true
	}

	// Initially evaluate node liveness status, so we filter the nodes to check.
	var nodesToCheck []roachpb.NodeID
	vitality, err := s.nodeLiveness.ScanNodeVitalityFromKV(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	resp := &serverpb.DecommissionPreCheckResponse{}
	resultsByNodeID := make(map[roachpb.NodeID]serverpb.DecommissionPreCheckResponse_NodeCheckResult)

	// Any nodes that are already decommissioned or have unknown liveness should
	// not be checked, and are added to response without replica counts or errors.
	// TODO(baptist): Revisit if the handling of unknown nodes is correct.
	for _, nID := range req.NodeIDs {
		vitality := vitality[nID]
		if vitality.IsDecommissioned() {
			resultsByNodeID[nID] = serverpb.DecommissionPreCheckResponse_NodeCheckResult{
				NodeID:                nID,
				DecommissionReadiness: serverpb.DecommissionPreCheckResponse_ALREADY_DECOMMISSIONED,
			}
		} else if vitality.LivenessStatus() == livenesspb.NodeLivenessStatus_UNKNOWN {
			resultsByNodeID[nID] = serverpb.DecommissionPreCheckResponse_NodeCheckResult{
				NodeID:                nID,
				DecommissionReadiness: serverpb.DecommissionPreCheckResponse_UNKNOWN,
			}
		} else {
			nodesToCheck = append(nodesToCheck, nID)
		}
	}

	results, err := s.server.DecommissionPreCheck(ctx, nodesToCheck, req.StrictReadiness, collectTraces, int(req.NumReplicaReport))
	if err != nil {
		return nil, err
	}

	// Collect ranges that encountered errors by the nodes on which their replicas
	// exist. Ranges with replicas on multiple checked nodes will result in the
	// error being reported for each nodeID.
	rangeCheckErrsByNode := make(map[roachpb.NodeID][]serverpb.DecommissionPreCheckResponse_RangeCheckResult)
	for _, rangeWithErr := range results.RangesNotReady {
		rangeCheckResult := serverpb.DecommissionPreCheckResponse_RangeCheckResult{
			RangeID: rangeWithErr.Desc.RangeID,
			Action:  rangeWithErr.Action,
			Events:  recordedSpansToTraceEvents(rangeWithErr.TracingSpans),
			Error:   rangeWithErr.Err.Error(),
		}

		for _, nID := range nodesToCheck {
			if rangeWithErr.Desc.Replicas().HasReplicaOnNode(nID) {
				rangeCheckErrsByNode[nID] = append(rangeCheckErrsByNode[nID], rangeCheckResult)
			}
		}
	}

	// Evaluate readiness by validating that there are no ranges with replicas on
	// the given node(s) that did not pass checks.
	for _, nID := range nodesToCheck {
		numReplicas := len(results.ReplicasByNode[nID])
		var readiness serverpb.DecommissionPreCheckResponse_NodeReadiness
		if len(rangeCheckErrsByNode[nID]) > 0 {
			readiness = serverpb.DecommissionPreCheckResponse_ALLOCATION_ERRORS
		} else {
			readiness = serverpb.DecommissionPreCheckResponse_READY
		}

		resultsByNodeID[nID] = serverpb.DecommissionPreCheckResponse_NodeCheckResult{
			NodeID:                nID,
			DecommissionReadiness: readiness,
			ReplicaCount:          int64(numReplicas),
			CheckedRanges:         rangeCheckErrsByNode[nID],
		}
	}

	// Reorder checked nodes to match request order.
	for _, nID := range req.NodeIDs {
		resp.CheckedNodes = append(resp.CheckedNodes, resultsByNodeID[nID])
	}

	return resp, nil
}

// DecommissionStatus returns the DecommissionStatus for all or the given nodes.
func (s *systemAdminServer) DecommissionStatus(
	ctx context.Context, req *serverpb.DecommissionStatusRequest,
) (*serverpb.DecommissionStatusResponse, error) {
	r, err := s.decommissionStatusHelper(ctx, req)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return r, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *systemAdminServer) decommissionStatusHelper(
	ctx context.Context, req *serverpb.DecommissionStatusRequest,
) (*serverpb.DecommissionStatusResponse, error) {
	// Get the number of replicas on each node. We *may* not need all of them,
	// but that would be more complicated than seems worth it right now.
	nodeIDs := req.NodeIDs
	numReplicaReport := req.NumReplicaReport

	// If no nodeIDs given, use all nodes.
	if len(nodeIDs) == 0 {
		ns, err := s.server.status.ListNodesInternal(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return nil, errors.Wrap(err, "loading node statuses")
		}
		for _, status := range ns.Nodes {
			nodeIDs = append(nodeIDs, status.Desc.NodeID)
		}
	}

	// If the client specified a number of decommissioning replicas to report,
	// prepare to get decommissioning replicas to report to the operator.
	// numReplicaReport is the number of replicas reported for each node.
	var replicasToReport map[roachpb.NodeID][]*serverpb.DecommissionStatusResponse_Replica
	if numReplicaReport > 0 {
		log.Ops.Warning(ctx, "possible decommission stall detected")
		replicasToReport = make(map[roachpb.NodeID][]*serverpb.DecommissionStatusResponse_Replica)
	}

	isDecommissioningNode := func(n roachpb.NodeID) bool {
		for _, nodeID := range nodeIDs {
			if n == nodeID {
				return true
			}
		}
		return false
	}

	// Compute the replica counts for the target nodes only. This map doubles as
	// a lookup table to check whether we care about a given node.
	var replicaCounts map[roachpb.NodeID]int64
	if err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		const pageSize = 10000
		replicaCounts = make(map[roachpb.NodeID]int64)
		for _, nodeID := range nodeIDs {
			replicaCounts[nodeID] = 0
		}
		return txn.Iterate(ctx, keys.Meta2Prefix, keys.MetaMax, pageSize,
			func(rows []kv.KeyValue) error {
				rangeDesc := roachpb.RangeDescriptor{}
				for _, row := range rows {
					if err := row.ValueProto(&rangeDesc); err != nil {
						return errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
					}
					for _, r := range rangeDesc.Replicas().Descriptors() {
						if numReplicaReport > 0 {
							if len(replicasToReport[r.NodeID]) < int(numReplicaReport) {
								if isDecommissioningNode(r.NodeID) {
									replicasToReport[r.NodeID] = append(replicasToReport[r.NodeID],
										&serverpb.DecommissionStatusResponse_Replica{
											ReplicaID: r.ReplicaID,
											RangeID:   rangeDesc.RangeID,
										},
									)
									log.Ops.Warningf(ctx,
										"n%d still has replica id %d for range r%d",
										r.NodeID,
										r.ReplicaID,
										rangeDesc.RangeID,
									)
								}
							}
						}
						if _, ok := replicaCounts[r.NodeID]; ok {
							replicaCounts[r.NodeID]++
						}
					}
				}
				return nil
			})
	}); err != nil {
		return nil, err
	}

	var res serverpb.DecommissionStatusResponse
	// We use ScanNodeVitalityFromKV to avoid races in which the caller has
	// just made an update to a liveness record but has not received this
	// update in its local liveness instance yet. Doing a consistent read
	// here avoids such issues.
	//
	// For an example, see:
	//
	// https://github.com/cockroachdb/cockroach/issues/73636
	vitalityMap, err := s.nodeLiveness.ScanNodeVitalityFromKV(ctx)
	if err != nil {
		return nil, err
	}

	for nodeID := range replicaCounts {
		l, ok := vitalityMap[nodeID]
		if !ok {
			return nil, errors.Newf("unable to get liveness for %d", nodeID)
		}
		nodeResp := serverpb.DecommissionStatusResponse_Status{
			NodeID:           nodeID,
			ReplicaCount:     replicaCounts[nodeID],
			Membership:       l.MembershipStatus(),
			Draining:         l.IsDraining(),
			ReportedReplicas: replicasToReport[nodeID],
		}
		if l.IsLive(livenesspb.DecommissionCheck) {
			nodeResp.IsLive = true
		}
		res.Status = append(res.Status, nodeResp)
	}

	sort.Slice(res.Status, func(i, j int) bool {
		return res.Status[i].NodeID < res.Status[j].NodeID
	})

	return &res, nil
}

// Decommission sets the decommission flag to the specified value on the specified node(s).
// When the flag is set to DECOMMISSIONED, an empty response is returned on success -- this
// ensures a node can decommission itself, since the node could otherwise lose RPC access
// to the cluster while building the full response.
func (s *systemAdminServer) Decommission(
	ctx context.Context, req *serverpb.DecommissionRequest,
) (*serverpb.DecommissionStatusResponse, error) {
	nodeIDs := req.NodeIDs

	if len(nodeIDs) == 0 {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "no node ID specified")
	}

	// Mark the target nodes with their new membership status. They'll find out
	// as they heartbeat their liveness.
	if err := s.server.Decommission(ctx, req.TargetMembership, nodeIDs); err != nil {
		// NB: not using srverrors.ServerError() here since Decommission
		// already returns a proper gRPC error status.
		return nil, err
	}

	// We return an empty response when setting the final DECOMMISSIONED state,
	// since a node can be asked to decommission itself which may cause it to
	// lose access to cluster RPC and fail to populate the response.
	if req.TargetMembership == livenesspb.MembershipStatus_DECOMMISSIONED {
		return &serverpb.DecommissionStatusResponse{}, nil
	}

	return s.DecommissionStatus(ctx, &serverpb.DecommissionStatusRequest{NodeIDs: nodeIDs, NumReplicaReport: req.NumReplicaReport})
}

// DataDistribution returns a count of replicas on each node for each table.
func (s *adminServer) DataDistribution(
	ctx context.Context, req *serverpb.DataDistributionRequest,
) (_ *serverpb.DataDistributionResponse, retErr error) {
	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	userName, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	r, err := s.dataDistributionHelper(ctx, req, userName)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return r, nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) dataDistributionHelper(
	ctx context.Context, req *serverpb.DataDistributionRequest, userName username.SQLUsername,
) (resp *serverpb.DataDistributionResponse, retErr error) {
	resp = &serverpb.DataDistributionResponse{
		DatabaseInfo: make(map[string]serverpb.DataDistributionResponse_DatabaseInfo),
		ZoneConfigs:  make(map[string]serverpb.DataDistributionResponse_ZoneConfig),
	}

	// We use crdb_internal.tables as it also returns data for deleted tables
	// which are not garbage collected yet, as opposed to information_schema,
	// because we are interested in the data for all ranges, not just ranges for
	// visible tables.
	//
	// The query is structured as follows:
	//
	// 1. The tables CTE selects table details from crdb_internal.tables and
	//    joins it with crdb_internal.table_spans to get the start and end keys for
	//    each table. We exclude tables with a NULL database_name to avoid virtual
	//    tables (like crdb_internal.tables itself).
	//
	// 2. The main SELECT joins the tables CTE with crdb_internal.ranges_no_leases to
	//    get the ranges the current table overlaps with. A single table can
	//    overlap with multiple ranges, and if range coalescing is enabled, a single
	//    range may overlap with multiple tables too.
	tablesQuery := `
    WITH tables AS (
        SELECT
            t.schema_name, t.name AS table_name, t.database_name,
            t.table_id, t.drop_time, s.start_key, s.end_key
        FROM
            "".crdb_internal.tables t
            JOIN "".crdb_internal.table_spans s ON t.table_id = s.descriptor_id
        WHERE
            t.database_name IS NOT NULL
    )
    SELECT
        t.table_id, t.table_name, t.schema_name, t.database_name, t.drop_time, r.replicas
    FROM
        tables t
        JOIN "".crdb_internal.ranges_no_leases r ON t.start_key < r.end_key
            AND t.end_key > r.start_key
    ORDER BY t.table_id;`

	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "data-distribution", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		tablesQuery,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	var hasNext bool
	for hasNext, err = it.Next(ctx); err == nil && hasNext; /* `it` updated by inner loop */ {
		firstRow := it.Cur()
		tableID := uint32(*firstRow[0].(*tree.DInt))

		tableInfo := serverpb.DataDistributionResponse_TableInfo{
			ReplicaCountByNodeId: make(map[roachpb.NodeID]int64),
		}

		// Iterate over rows with the same table ID since rows are sorted by table_id
		for ; err == nil && hasNext; hasNext, err = it.Next(ctx) {
			row := it.Cur()
			curTableID := uint32(*row[0].(*tree.DInt))
			if tableID != curTableID {
				break
			}
			for _, node := range row[5].(*tree.DArray).Array {
				tableInfo.ReplicaCountByNodeId[roachpb.NodeID(*node.(*tree.DInt))]++
			}
		}

		if droppedAtDatum, ok := firstRow[4].(*tree.DTimestamp); ok {
			tableInfo.DroppedAt = &droppedAtDatum.Time
		}

		tableName := (*string)(firstRow[1].(*tree.DString))
		schemaName := (*string)(firstRow[2].(*tree.DString))
		fqTableName := fmt.Sprintf("%s.%s", tree.NameStringP(schemaName), tree.NameStringP(tableName))

		dbName := (*string)(firstRow[3].(*tree.DString))
		// Insert database if it doesn't exist.
		dbInfo, ok := resp.DatabaseInfo[*dbName]
		if !ok {
			dbInfo = serverpb.DataDistributionResponse_DatabaseInfo{
				TableInfo: make(map[string]serverpb.DataDistributionResponse_TableInfo),
			}
			resp.DatabaseInfo[*dbName] = dbInfo
		}
		dbInfo.TableInfo[fqTableName] = tableInfo
	}
	if err != nil {
		return nil, err
	}

	// Get zone configs.
	// TODO(vilterp): this can be done in parallel with getting table/db names and replica counts.
	zoneConfigsQuery := `
		SELECT target, raw_config_sql, raw_config_protobuf
		FROM crdb_internal.zones
		WHERE target IS NOT NULL
	`
	it, err = s.internalExecutor.QueryIteratorEx(
		ctx, "data-distribution", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		zoneConfigsQuery)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it isql.Rows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	for hasNext, err = it.Next(ctx); hasNext; hasNext, err = it.Next(ctx) {
		row := it.Cur()
		target := string(tree.MustBeDString(row[0]))
		var zcSQL string
		if zcSQLDatum, ok := tree.AsDString(row[1]); ok {
			zcSQL = string(zcSQLDatum)
		}
		zcBytes := tree.MustBeDBytes(row[2])
		var zcProto zonepb.ZoneConfig
		if err := protoutil.Unmarshal([]byte(zcBytes), &zcProto); err != nil {
			return nil, err
		}

		resp.ZoneConfigs[target] = serverpb.DataDistributionResponse_ZoneConfig{
			Target:    target,
			Config:    zcProto,
			ConfigSQL: zcSQL,
		}
	}
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// EnqueueRange runs the specified range through the specified queue, returning
// the detailed trace and error information from doing so.
func (s *systemAdminServer) EnqueueRange(
	ctx context.Context, req *serverpb.EnqueueRangeRequest,
) (*serverpb.EnqueueRangeResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireRepairClusterPermission(ctx); err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}

	if req.NodeID < 0 {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "node_id must be non-negative; got %d", req.NodeID)
	}
	if req.Queue == "" {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "queue name must be non-empty")
	}
	if req.RangeID <= 0 {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "range_id must be positive; got %d", req.RangeID)
	}

	// If the request is targeted at this node, serve it directly. Otherwise,
	// forward it to the appropriate node(s). If no node was specified, forward
	// it to all nodes.
	if req.NodeID == roachpb.NodeID(s.serverIterator.getID()) {
		return s.enqueueRangeLocal(ctx, req)
	} else if req.NodeID != 0 {
		admin, err := s.dialNode(ctx, req.NodeID)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		return admin.EnqueueRange(ctx, req)
	}

	response := &serverpb.EnqueueRangeResponse{}

	dialFn := func(ctx context.Context, nodeID roachpb.NodeID) (interface{}, error) {
		client, err := s.dialNode(ctx, nodeID)
		return client, err
	}
	nodeFn := func(ctx context.Context, client interface{}, nodeID roachpb.NodeID) (interface{}, error) {
		admin := client.(serverpb.AdminClient)
		req := *req
		req.NodeID = nodeID
		return admin.EnqueueRange(ctx, &req)
	}
	responseFn := func(_ roachpb.NodeID, nodeResp interface{}) {
		nodeDetails := nodeResp.(*serverpb.EnqueueRangeResponse)
		response.Details = append(response.Details, nodeDetails.Details...)
	}
	errorFn := func(nodeID roachpb.NodeID, err error) {
		errDetail := &serverpb.EnqueueRangeResponse_Details{
			NodeID: nodeID,
			Error:  err.Error(),
		}
		response.Details = append(response.Details, errDetail)
	}

	if err := timeutil.RunWithTimeout(ctx, "enqueue range", time.Minute, func(ctx context.Context) error {
		return iterateNodes(
			ctx, s.serverIterator, s.server.stopper, redact.Sprintf("enqueue r%d in queue %s", req.RangeID, req.Queue),
			noTimeout,
			dialFn, nodeFn, responseFn, errorFn,
		)
	}); err != nil {
		if len(response.Details) == 0 {
			return nil, srverrors.ServerError(ctx, err)
		}
		response.Details = append(response.Details, &serverpb.EnqueueRangeResponse_Details{
			Error: err.Error(),
		})
	}

	return response, nil
}

// enqueueRangeLocal checks whether the local node has a replica for the
// requested range that can be run through the queue, running it through the
// queue and returning trace/error information if so. If not, returns an empty
// response.
//
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *systemAdminServer) enqueueRangeLocal(
	ctx context.Context, req *serverpb.EnqueueRangeRequest,
) (*serverpb.EnqueueRangeResponse, error) {
	response := &serverpb.EnqueueRangeResponse{
		Details: []*serverpb.EnqueueRangeResponse_Details{
			{
				NodeID: roachpb.NodeID(s.serverIterator.getID()),
			},
		},
	}

	var store *kvserver.Store
	var repl *kvserver.Replica
	if err := s.server.node.stores.VisitStores(func(s *kvserver.Store) error {
		r := s.GetReplicaIfExists(req.RangeID)
		if r == nil {
			return nil
		}
		repl = r
		store = s
		return nil
	}); err != nil {
		response.Details[0].Error = err.Error()
		return response, nil
	}

	if store == nil || repl == nil {
		response.Details[0].Error = fmt.Sprintf("n%d has no replica for r%d", s.server.NodeID(), req.RangeID)
		return response, nil
	}

	// Handle mixed-version clusters across the "gc" to "mvccGC" queue rename.
	// TODO(nvanbenschoten): remove this in v23.1. Inline req.Queue again.
	// The client logic in pkg/ui/workspaces/db-console/src/views/reports/containers/enqueueRange/index.tsx
	// should stop sending "gc" in v22.2. When removing, confirm that the
	// associated TODO in index.tsx was addressed in the previous release.
	//
	// Explanation of upgrade:
	// - v22.1 will understand "gc" and "mvccGC" on the server. Its client will
	//   continue to send "gc" to interop with v21.2 servers.
	// - v22.2's client will send "mvccGC" but will still have to understand "gc"
	//   on the server to deal with v22.1 clients.
	// - v23.1's server can stop understanding "gc".
	queueName := req.Queue
	if strings.ToLower(queueName) == "gc" {
		queueName = "mvccGC"
	}

	traceCtx, rec := tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "trace-enqueue")
	processErr, err := store.Enqueue(
		traceCtx, queueName, repl, req.SkipShouldQueue, false, /* async */
	)
	traceSpans := rec()
	if err != nil {
		response.Details[0].Error = err.Error()
		return response, nil
	}
	response.Details[0].Events = recordedSpansToTraceEvents(traceSpans)
	if processErr != nil {
		response.Details[0].Error = processErr.Error()
	}
	return response, nil
}

// SendKVBatch proxies the given BatchRequest into KV, returning the
// response. It is for use by the CLI `debug send-kv-batch` command.
func (s *systemAdminServer) SendKVBatch(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	// Note: the root user will bypass SQL auth checks, which is useful in case of
	// a cluster outage.
	err := s.privilegeChecker.RequireRepairClusterPermission(ctx)
	if err != nil {
		// NB: not using srverrors.ServerError() here since the priv checker
		// already returns a proper gRPC error status.
		return nil, err
	}
	if ba == nil {
		return nil, grpcstatus.Errorf(codes.InvalidArgument, "BatchRequest cannot be nil")
	}

	user, err := authserver.UserFromIncomingRPCContext(ctx)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}

	// Emit a structured log event for the call.
	jsonpb := protoutil.JSONPb{}
	baJSON, err := jsonpb.Marshal(ba)
	if err != nil {
		return nil, srverrors.ServerError(ctx, errors.Wrap(err, "failed to encode BatchRequest as JSON"))
	}
	event := &eventpb.DebugSendKvBatch{
		CommonEventDetails: logpb.CommonEventDetails{
			Timestamp: timeutil.Now().UnixNano(),
		},
		CommonDebugEventDetails: eventpb.CommonDebugEventDetails{
			NodeID: int32(s.server.NodeID()),
			User:   user.Normalized(),
		},
		BatchRequest: string(baJSON),
	}
	log.StructuredEvent(ctx, severity.INFO, event)

	ctx, sp := s.server.node.setupSpanForIncomingRPC(ctx, roachpb.SystemTenantID, ba)
	// Wipe the tracing information from the request. We've used this info in the
	// setupSpanForIncomingRPC() call above; from now on the request is traced as
	// per the span we just created.
	ba.TraceInfo = nil
	var br *kvpb.BatchResponse
	// NB: wrapped to delay br evaluation to its value when returning.
	defer func() {
		var redact redactOpt
		if RedactServerTracesForSecondaryTenants.Get(&s.server.ClusterSettings().SV) {
			redact = redactIfTenantRequest
		} else {
			redact = dontRedactEvenIfTenantRequest
		}
		sp.finish(br, redact)
	}()
	br, pErr := s.db.NonTransactionalSender().Send(ctx, ba)
	if br == nil {
		br = &kvpb.BatchResponse{}
	}
	br.Error = pErr
	return br, nil
}

func (s *systemAdminServer) RecoveryCollectReplicaInfo(
	request *serverpb.RecoveryCollectReplicaInfoRequest,
	stream serverpb.Admin_RecoveryCollectReplicaInfoServer,
) error {
	ctx := stream.Context()
	ctx = s.server.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return err
	}
	log.Ops.Info(ctx, "streaming cluster replica recovery info")

	return s.server.recoveryServer.ServeClusterReplicas(ctx, request, stream, s.server.db)
}

func (s *systemAdminServer) RecoveryCollectLocalReplicaInfo(
	request *serverpb.RecoveryCollectLocalReplicaInfoRequest,
	stream serverpb.Admin_RecoveryCollectLocalReplicaInfoServer,
) error {
	ctx := stream.Context()
	ctx = s.server.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return err
	}

	log.Ops.Info(ctx, "streaming local replica recovery info")
	return s.server.recoveryServer.ServeLocalReplicas(ctx, request, stream)
}

func (s *systemAdminServer) RecoveryStagePlan(
	ctx context.Context, request *serverpb.RecoveryStagePlanRequest,
) (*serverpb.RecoveryStagePlanResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireRepairClusterPermission(ctx)
	if err != nil {
		return nil, err
	}

	log.Ops.Info(ctx, "staging recovery plan")
	return s.server.recoveryServer.StagePlan(ctx, request)
}

func (s *systemAdminServer) RecoveryNodeStatus(
	ctx context.Context, request *serverpb.RecoveryNodeStatusRequest,
) (*serverpb.RecoveryNodeStatusResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	return s.server.recoveryServer.NodeStatus(ctx, request)
}

func (s *systemAdminServer) RecoveryVerify(
	ctx context.Context, request *serverpb.RecoveryVerifyRequest,
) (*serverpb.RecoveryVerifyResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
	if err != nil {
		return nil, err
	}

	return s.server.recoveryServer.Verify(ctx, request, s.nodeLiveness, s.db)
}

// resultScanner scans columns from sql.ResultRow instances into variables,
// performing the appropriate casting and error detection along the way.
type resultScanner struct {
	colNameToIdx map[string]int
}

func makeResultScanner(cols []colinfo.ResultColumn) resultScanner {
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
func (rs resultScanner) IsNull(row tree.Datums, col string) (bool, error) {
	idx, ok := rs.colNameToIdx[col]
	if !ok {
		return false, errors.Errorf("result is missing column %s", col)
	}
	return row[idx] == tree.DNull, nil
}

// ScanIndex scans the given column index of the given row into dst.
func (rs resultScanner) ScanIndex(row tree.Datums, index int, dst interface{}) error {
	src := row[index]

	if dst == nil {
		return errors.Errorf("nil destination pointer passed in")
	}

	switch d := dst.(type) {
	case *string:
		s, ok := tree.AsDString(src)
		if !ok {
			return errors.Errorf("source type assertion failed %d %T", index, src)
		}
		*d = string(s)

	case **string:
		s, ok := tree.AsDString(src)
		if !ok {
			if src != tree.DNull {
				return errors.Errorf("source type assertion failed")
			}
			*d = nil
			break
		}
		val := string(s)
		*d = &val

	case *bool:
		s, ok := src.(*tree.DBool)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = bool(*s)

	case *float32:
		s, ok := src.(*tree.DFloat)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = float32(*s)

	case **float32:
		s, ok := src.(*tree.DFloat)
		if !ok {
			if src != tree.DNull {
				return errors.Errorf("source type assertion failed")
			}
			*d = nil
			break
		}
		val := float32(*s)
		*d = &val

	case *int64:
		s, ok := tree.AsDInt(src)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = int64(s)

	case **int64:
		s, ok := src.(*tree.DInt)
		if !ok {
			if src != tree.DNull {
				return errors.Errorf("source type assertion failed")
			}
			*d = nil
			break
		}
		val := int64(*s)
		*d = &val

	case *[]int64:
		s, ok := tree.AsDArray(src)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		for i := 0; i < s.Len(); i++ {
			id, ok := tree.AsDInt(s.Array[i])
			if !ok {
				return errors.Errorf("source type assertion failed on index %d", i)
			}
			*d = append(*d, int64(id))
		}

	case *[]descpb.ID:
		s, ok := tree.AsDArray(src)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		for i := 0; i < s.Len(); i++ {
			id, ok := tree.AsDInt(s.Array[i])
			if !ok {
				return errors.Errorf("source type assertion failed on index %d", i)
			}
			*d = append(*d, descpb.ID(id))
		}

	case *time.Time:
		switch s := src.(type) {
		case *tree.DTimestamp:
			*d = s.Time
		case *tree.DTimestampTZ:
			*d = s.Time
		default:
			return errors.Errorf("source type assertion failed")
		}

	// Passing a **time.Time instead of a *time.Time means the source is allowed
	// to be NULL, in which case nil is stored into *src.
	case **time.Time:
		switch s := src.(type) {
		case *tree.DTimestamp:
			*d = &s.Time
		case *tree.DTimestampTZ:
			*d = &s.Time
		default:
			if src == tree.DNull {
				*d = nil
			} else {
				return errors.Errorf("source type assertion failed")
			}
		}

	case *[]byte:
		s, ok := src.(*tree.DBytes)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		// Yes, this copies, but this probably isn't in the critical path.
		*d = []byte(*s)

	case *apd.Decimal:
		s, ok := src.(*tree.DDecimal)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = s.Decimal

	case **apd.Decimal:
		s, ok := src.(*tree.DDecimal)
		if !ok {
			if src != tree.DNull {
				return errors.Errorf("source type assertion failed")
			}
			*d = nil
			break
		}
		*d = &s.Decimal

	default:
		return errors.Errorf("unimplemented type for scanCol: %T", dst)
	}

	return nil
}

// ScanAll scans all the columns from the given row, in order, into dsts.
func (rs resultScanner) ScanAll(row tree.Datums, dsts ...interface{}) error {
	if len(row) != len(dsts) {
		return fmt.Errorf(
			"ScanAll: row has %d columns but %d dests provided", len(row), len(dsts))
	}
	for i := 0; i < len(row); i++ {
		if err := rs.ScanIndex(row, i, dsts[i]); err != nil {
			return err
		}
	}
	return nil
}

// Scan scans the column with the given name from the given row into dst.
func (rs resultScanner) Scan(row tree.Datums, colName string, dst interface{}) error {
	idx, ok := rs.colNameToIdx[colName]
	if !ok {
		return errors.Errorf("result is missing column %s", colName)
	}
	return rs.ScanIndex(row, idx, dst)
}

// TODO(mrtracy): The following methods, used to look up the zone configuration
// for a database or table, use the same algorithm as a set of methods in
// cli/zone.go for the same purpose. However, as that code connects to the
// server with a SQL connections, while this code uses the Executor, the
// code cannot be commonized.
//
// queryZone retrieves the specific ZoneConfig associated with the supplied ID,
// if it exists.
//
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) queryZone(
	ctx context.Context, userName username.SQLUsername, id descpb.ID,
) (zonepb.ZoneConfig, bool, error) {
	const query = `SELECT crdb_internal.get_zone_config($1)`
	row, cols, err := s.internalExecutor.QueryRowExWithCols(
		ctx,
		"admin-query-zone",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		query,
		id,
	)
	if err != nil {
		return *zonepb.NewZoneConfig(), false, err
	}

	if row == nil {
		return *zonepb.NewZoneConfig(), false, errors.Errorf("invalid number of rows (0) returned: %s (%d)", query, id)
	}

	var zoneBytes []byte
	scanner := makeResultScanner(cols)
	if isNull, err := scanner.IsNull(row, cols[0].Name); err != nil {
		return *zonepb.NewZoneConfig(), false, err
	} else if isNull {
		return *zonepb.NewZoneConfig(), false, nil
	}

	err = scanner.ScanIndex(row, 0, &zoneBytes)
	if err != nil {
		return *zonepb.NewZoneConfig(), false, err
	}

	var zone zonepb.ZoneConfig
	if err := protoutil.Unmarshal(zoneBytes, &zone); err != nil {
		return *zonepb.NewZoneConfig(), false, err
	}
	return zone, true, nil
}

// queryZonePath queries a path of sql object IDs, as generated by
// queryDescriptorIDPath(), for a ZoneConfig. It returns the most specific
// ZoneConfig specified for the object IDs in the path.
//
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) queryZonePath(
	ctx context.Context, userName username.SQLUsername, path []descpb.ID,
) (descpb.ID, zonepb.ZoneConfig, bool, error) {
	for i := len(path) - 1; i >= 0; i-- {
		zone, zoneExists, err := s.queryZone(ctx, userName, path[i])
		if err != nil || zoneExists {
			return path[i], zone, true, err
		}
	}
	return 0, *zonepb.NewZoneConfig(), false, nil
}

// queryDatabaseID queries for the ID of the database with the given name.
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) queryDatabaseID(
	ctx context.Context, userName username.SQLUsername, name string,
) (descpb.ID, error) {
	const query = `SELECT crdb_internal.get_database_id($1)`
	row, cols, err := s.internalExecutor.QueryRowExWithCols(
		ctx, "admin-query-namespace-ID", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		query, name,
	)
	if err != nil {
		return 0, err
	}

	if row == nil {
		return 0, errors.Errorf("invalid number of rows (0) returned: %s (%s)", query, name)
	}

	var id int64
	scanner := makeResultScanner(cols)
	if isNull, err := scanner.IsNull(row, cols[0].Name); err != nil {
		return 0, err
	} else if isNull {
		return 0, errors.Errorf("database %s not found", name)
	}

	err = scanner.ScanIndex(row, 0, &id)
	if err != nil {
		return 0, err
	}

	return descpb.ID(id), nil
}

// queryTableID queries for the ID of the table with the given name in the
// database with the given name. The table name may contain a schema qualifier.
// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
//
// TODO(clust-obs): This method should not be implemented on top of
// `adminServer`. There should be a better place for it.
func (s *adminServer) queryTableID(
	ctx context.Context, username username.SQLUsername, database string, tableName string,
) (descpb.ID, error) {
	row, err := s.internalExecutor.QueryRowEx(
		ctx, "admin-resolve-name", nil,
		sessiondata.InternalExecutorOverride{User: username, Database: database},
		"SELECT $1::regclass::oid", tableName,
	)
	if err != nil {
		return descpb.InvalidID, err
	}
	if row == nil {
		return descpb.InvalidID, errors.Newf("failed to resolve %q as a table name", tableName)
	}
	return descpb.ID(tree.MustBeDOid(row[0]).Oid), nil
}

// Note that the function returns plain errors, and it is the caller's
// responsibility to convert them to srverrors.ServerErrors.
func (s *adminServer) dialNode(
	ctx context.Context, nodeID roachpb.NodeID,
) (serverpb.AdminClient, error) {
	conn, err := s.serverIterator.dialNode(ctx, serverID(nodeID))
	if err != nil {
		return nil, err
	}
	return serverpb.NewAdminClient(conn), nil
}

func (s *adminServer) ListTracingSnapshots(
	ctx context.Context, req *serverpb.ListTracingSnapshotsRequest,
) (*serverpb.ListTracingSnapshotsResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewDebugPermission(ctx)
	if err != nil {
		return nil, err
	}

	snapshotInfo := s.sqlServer.cfg.Tracer.GetSnapshots()
	autoSnapshotInfo := s.sqlServer.cfg.Tracer.GetAutomaticSnapshots()
	snapshots := make([]*serverpb.SnapshotInfo, 0, len(snapshotInfo)+len(autoSnapshotInfo))
	for i := range snapshotInfo {
		si := snapshotInfo[i]
		snapshots = append(snapshots, &serverpb.SnapshotInfo{
			SnapshotID: int64(si.ID),
			CapturedAt: &si.CapturedAt,
		})
	}
	for i := range autoSnapshotInfo {
		si := autoSnapshotInfo[i]
		snapshots = append(snapshots, &serverpb.SnapshotInfo{
			// Flip the IDs of automatic snapshots to negative so we can tell when the
			// client asks for one of these that we should look for it in the auto
			// snapshots not the regular ones.
			SnapshotID: int64(si.ID * -1),
			CapturedAt: &si.CapturedAt,
		})
	}
	resp := &serverpb.ListTracingSnapshotsResponse{
		Snapshots: snapshots,
	}
	return resp, nil
}

// TakeTracingSnapshot captures a new snapshot of the Active Spans Registry.
// The new snapshot is returned, and also made available through
// ListTracingSnapshots.
func (s *adminServer) TakeTracingSnapshot(
	ctx context.Context, req *serverpb.TakeTracingSnapshotRequest,
) (*serverpb.TakeTracingSnapshotResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewDebugPermission(ctx)
	if err != nil {
		return nil, err
	}

	snapshot := s.sqlServer.cfg.Tracer.SaveSnapshot()
	resp := &serverpb.TakeTracingSnapshotResponse{
		Snapshot: &serverpb.SnapshotInfo{
			SnapshotID: int64(snapshot.ID),
			CapturedAt: &snapshot.CapturedAt,
		},
	}
	return resp, nil
}

func getSpanTag(t tracingui.ProcessedTag) *serverpb.SpanTag {
	processedChildren := make([]*serverpb.ChildSpanTag, len(t.Children))
	for i, child := range t.Children {
		processedChildren[i] = &serverpb.ChildSpanTag{
			Key: child.Key,
			Val: child.Val,
		}
	}
	return &serverpb.SpanTag{
		Key:             t.Key,
		Val:             t.Val,
		Caption:         t.Caption,
		Link:            t.Link,
		Hidden:          t.Hidden,
		Highlight:       t.Highlight,
		Inherit:         t.Inherit,
		Inherited:       t.Inherited,
		PropagateUp:     t.PropagateUp,
		CopiedFromChild: t.CopiedFromChild,
		Children:        processedChildren,
	}
}

// GetTracingSnapshot returns a snapshot of the tracing spans in the active
// spans registry previously generated through TakeTracingSnapshots.
func (s *adminServer) GetTracingSnapshot(
	ctx context.Context, req *serverpb.GetTracingSnapshotRequest,
) (*serverpb.GetTracingSnapshotResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewDebugPermission(ctx)
	if err != nil {
		return nil, err
	}

	id := tracing.SnapshotID(req.SnapshotId)
	tr := s.sqlServer.cfg.Tracer
	var snapshot tracing.SpansSnapshot
	// If the ID is negative it indicates it is an automatic snapshot, so flip it
	// back to positive and fetch it from the automatic API instead.
	if id < 0 {
		snapshot, err = tr.GetAutomaticSnapshot(id * -1)
	} else {
		snapshot, err = tr.GetSnapshot(id)
	}
	if err != nil {
		return nil, err
	}

	spansList := tracingui.ProcessSnapshot(snapshot, tr.GetActiveSpansRegistry())

	spans := make([]*serverpb.TracingSpan, len(spansList.Spans))
	stacks := make(map[string]string)

	for i, s := range spansList.Spans {
		tags := make([]*serverpb.SpanTag, len(s.Tags))
		for j, tag := range s.Tags {
			tags[j] = getSpanTag(tag)
		}
		childrenMetadata := make([]*serverpb.NamedOperationMetadata, len(s.ChildrenMetadata))

		j := 0
		for name, cm := range s.ChildrenMetadata {
			childrenMetadata[j] = &serverpb.NamedOperationMetadata{
				Name:     name,
				Metadata: cm,
			}
			j++
		}

		spans[i] = &serverpb.TracingSpan{
			Operation:            s.Operation,
			TraceID:              s.TraceID,
			SpanID:               s.SpanID,
			ParentSpanID:         s.ParentSpanID,
			Start:                s.Start,
			GoroutineID:          s.GoroutineID,
			ProcessedTags:        tags,
			Current:              s.Current,
			CurrentRecordingMode: s.CurrentRecordingMode.ToProto(),
			ChildrenMetadata:     childrenMetadata,
		}
	}

	for k, v := range spansList.Stacks {
		stacks[strconv.FormatInt(int64(k), 10)] = v
	}

	snapshotResp := &serverpb.TracingSnapshot{
		SnapshotID: int64(id),
		CapturedAt: &snapshot.CapturedAt,
		Spans:      spans,
		Stacks:     stacks,
	}
	resp := &serverpb.GetTracingSnapshotResponse{
		Snapshot: snapshotResp,
	}
	return resp, nil
}

// GetTrace returns the trace with a specified ID. Depending on the request, the
// trace is returned either from a snapshot that was previously taken, or
// directly from the active spans registry.
func (s *adminServer) GetTrace(
	ctx context.Context, req *serverpb.GetTraceRequest,
) (*serverpb.GetTraceResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	err := s.privilegeChecker.RequireViewDebugPermission(ctx)
	if err != nil {
		return nil, err
	}
	var recording tracingpb.Recording
	var snapshotID tracing.SnapshotID

	traceID := req.TraceID
	snapID := req.SnapshotID
	if snapID != 0 {
		snapshotID = tracing.SnapshotID(snapID)
		snapshot, err := s.sqlServer.cfg.Tracer.GetSnapshot(snapshotID)
		if err != nil {
			return nil, err
		}

		for _, r := range snapshot.Traces {
			if r[0].TraceID == traceID {
				recording = r
				break
			}
		}
	}
	// Look for the trace in the registry to see if it's present and read its
	// recording mode. If we were asked to display the current trace (as opposed
	// to the trace saved in a snapshot), we also collect the recording.
	traceStillExists := false
	if err := s.sqlServer.cfg.Tracer.SpanRegistry().VisitRoots(func(sp tracing.RegistrySpan) error {
		if sp.TraceID() != traceID {
			return nil
		}
		traceStillExists = true
		if recording == nil {
			trace := sp.GetFullRecording(tracingpb.RecordingVerbose)
			recording = trace.Flatten()
		}
		return iterutil.StopIteration()
	}); err != nil {
		return nil, err
	}

	if recording == nil {
		return nil, errors.Errorf("Trace %d not found.", traceID)
	}

	return &serverpb.GetTraceResponse{
		SnapshotID:          snapID,
		TraceID:             traceID,
		StillExists:         traceStillExists,
		SerializedRecording: recording.String(),
	}, nil
}

// SetTraceRecordingType updates the recording mode of all or some of the spans
// in a trace.
func (s *adminServer) SetTraceRecordingType(
	ctx context.Context, req *serverpb.SetTraceRecordingTypeRequest,
) (*serverpb.SetTraceRecordingTypeResponse, error) {
	if req.TraceID == 0 {
		return nil, errors.Errorf("missing trace id")
	}
	_ = s.sqlServer.cfg.Tracer.VisitSpans(func(sp tracing.RegistrySpan) error {
		if sp.TraceID() != req.TraceID {
			return nil
		}
		// NB: The recording type propagates to the children, recursively.
		sp.SetRecordingType(tracingpb.RecordingTypeFromProto(req.RecordingMode))
		return nil
	})
	return &serverpb.SetTraceRecordingTypeResponse{}, nil
}

// ListTenants returns a list of tenants that are served
// by shared-process services in this server.
func (s *systemAdminServer) ListTenants(
	ctx context.Context, _ *serverpb.ListTenantsRequest,
) (*serverpb.ListTenantsResponse, error) {
	tenantNames, err := s.server.serverController.getExpectedRunningTenants(ctx, s.internalExecutor)
	if err != nil {
		return nil, err
	}

	tenantList := make([]*serverpb.Tenant, 0, len(tenantNames))
	for _, tenantName := range tenantNames {
		server, _, err := s.server.serverController.getServer(ctx, tenantName)
		if err != nil {
			if errors.Is(err, errNoTenantServerRunning) {
				// The service for this tenant is not started yet. This is not
				// an error - the services are started asynchronously. The
				// client can try again later.
				continue
			}
			return nil, err
		}
		tenantID := server.getTenantID()
		tenantList = append(tenantList, &serverpb.Tenant{
			TenantId:   &tenantID,
			TenantName: string(tenantName),
			SqlAddr:    server.getSQLAddr(),
			RpcAddr:    server.getRPCAddr(),
		})
	}

	return &serverpb.ListTenantsResponse{
		Tenants: tenantList,
	}, nil
}

// ReadFromTenantInfo returns the read-from info for a tenant, if configured.
func (s *systemAdminServer) ReadFromTenantInfo(
	ctx context.Context, req *serverpb.ReadFromTenantInfoRequest,
) (*serverpb.ReadFromTenantInfoResponse, error) {
	tenantID, ok := roachpb.ClientTenantFromContext(ctx)
	if ok && req.TenantID != tenantID {
		return nil, errors.Errorf("mismatched tenant IDs")
	}
	tenantID = req.TenantID
	if tenantID.IsSystem() {
		return &serverpb.ReadFromTenantInfoResponse{}, nil
	}

	var dstID roachpb.TenantID
	var dstTenant *mtinfopb.TenantInfo
	if err := s.sqlServer.internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		found, err := sql.GetTenantRecordByID(ctx, txn, tenantID, s.st)
		if err != nil {
			return err
		}
		if found.ReadFromTenant == nil || !found.ReadFromTenant.IsSet() {
			return nil
		}
		dstID = *found.ReadFromTenant
		target, err := sql.GetTenantRecordByID(ctx, txn, dstID, s.st)
		if err != nil {
			return err
		}
		dstTenant = target
		return nil
	}); err != nil {
		return nil, err
	}

	if dstTenant == nil {
		return &serverpb.ReadFromTenantInfoResponse{}, nil
	}

	if dstTenant.PhysicalReplicationConsumerJobID == 0 {
		return nil, errors.Errorf("missing job ID")
	}

	progress, err := jobs.LoadJobProgress(ctx, s.sqlServer.internalDB, dstTenant.PhysicalReplicationConsumerJobID)
	if err != nil {
		return nil, err
	}

	return &serverpb.ReadFromTenantInfoResponse{ReadFrom: dstID, ReadAt: progress.GetStreamIngest().ReplicatedTime}, nil
}
