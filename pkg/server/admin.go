// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/ts/catalog"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	gwruntime "github.com/grpc-ecosystem/grpc-gateway/runtime"
	gwutil "github.com/grpc-ecosystem/grpc-gateway/utilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// adminPrefix is the prefix for RESTful endpoints used to provide an
	// administrative interface to the cockroach cluster.
	adminPrefix = "/_admin/v1/"

	// defaultAPIEventLimit is the default maximum number of events returned by any
	// endpoints returning events.
	defaultAPIEventLimit = 1000
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
		keys.PublicSchemaID,
		keys.TenantsRangesID,
	}))
}

// apiServerMessage is the standard body for all HTTP 500 responses.
var errAdminAPIError = status.Errorf(codes.Internal, "An internal server error "+
	"has occurred. Please check your CockroachDB logs for more details.")

// A adminServer provides a RESTful HTTP API to administration of
// the cockroach cluster.
type adminServer struct {
	*adminPrivilegeChecker
	server     *Server
	memMonitor *mon.BytesMonitor
}

// noteworthyAdminMemoryUsageBytes is the minimum size tracked by the
// admin SQL pool before the pool start explicitly logging overall
// usage growth in the log.
var noteworthyAdminMemoryUsageBytes = envutil.EnvOrDefaultInt64("COCKROACH_NOTEWORTHY_ADMIN_MEMORY_USAGE", 100*1024)

// newAdminServer allocates and returns a new REST server for
// administrative APIs.
func newAdminServer(s *Server, ie *sql.InternalExecutor) *adminServer {
	server := &adminServer{
		adminPrivilegeChecker: &adminPrivilegeChecker{ie: ie},
		server:                s,
	}
	// TODO(knz): We do not limit memory usage by admin operations
	// yet. Is this wise?
	server.memMonitor = mon.NewUnlimitedMonitor(
		context.Background(),
		"admin",
		mon.MemoryResource,
		nil,
		nil,
		noteworthyAdminMemoryUsageBytes,
		s.ClusterSettings(),
	)
	return server
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
		s.getStatementBundle(ctx, id, w)
	})

	// Register the endpoints defined in the proto.
	return serverpb.RegisterAdminHandler(ctx, mux, conn)
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

// isNotFoundError returns true if err is a table/database not found error.
func (s *adminServer) isNotFoundError(err error) bool {
	// TODO(cdo): Replace this crude suffix-matching with something more structured once we have
	// more structured errors.
	return err != nil && strings.HasSuffix(err.Error(), "does not exist")
}

// AllMetricMetadata returns all metrics' metadata.
func (s *adminServer) AllMetricMetadata(
	ctx context.Context, req *serverpb.MetricMetadataRequest,
) (*serverpb.MetricMetadataResponse, error) {

	resp := &serverpb.MetricMetadataResponse{
		Metadata: s.server.recorder.GetMetricsMetadata(),
	}

	return resp, nil
}

// ChartCatalog returns a catalog of Admin UI charts useful for debugging.
func (s *adminServer) ChartCatalog(
	ctx context.Context, req *serverpb.ChartCatalogRequest,
) (*serverpb.ChartCatalogResponse, error) {
	metricsMetadata := s.server.recorder.GetMetricsMetadata()

	chartCatalog, err := catalog.GenerateCatalog(metricsMetadata)

	if err != nil {
		return nil, err
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
	// All errors returned by this method must be serverErrors. We are careful
	// to not use serverError* methods in the body of the function, so we can
	// just do it here.
	defer func() {
		if retErr != nil {
			retErr = s.serverError(retErr)
		}
	}()

	ctx = s.server.AnnotateCtx(ctx)

	sessionUser, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.databasesHelper(ctx, req, sessionUser, 0, 0)
}

func (s *adminServer) databasesHelper(
	ctx context.Context,
	req *serverpb.DatabasesRequest,
	sessionUser security.SQLUsername,
	limit, offset int,
) (_ *serverpb.DatabasesResponse, retErr error) {
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-dbs", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sessionUser},
		"SHOW DATABASES",
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

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

func (s *adminServer) maybeHandleNotFoundError(err error) error {
	if s.isNotFoundError(err) {
		return status.Errorf(codes.NotFound, "%s", err)
	}
	if err != nil {
		return s.serverError(err)
	}
	return nil
}

// DatabaseDetails is an endpoint that returns grants and a list of table names
// for the specified database.
func (s *adminServer) DatabaseDetails(
	ctx context.Context, req *serverpb.DatabaseDetailsRequest,
) (_ *serverpb.DatabaseDetailsResponse, retErr error) {
	ctx = s.server.AnnotateCtx(ctx)
	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.databaseDetailsHelper(ctx, req, userName)
}

func (s *adminServer) getDatabaseGrants(
	ctx context.Context,
	req *serverpb.DatabaseDetailsRequest,
	userName security.SQLUsername,
	limit, offset int,
) (resp []serverpb.DatabaseDetailsResponse_Grant, retErr error) {
	escDBName := tree.NameStringP(&req.Database)
	// Placeholders don't work with SHOW statements, so we need to manually
	// escape the database name.
	//
	// TODO(cdo): Use placeholders when they're supported by SHOW.

	// Marshal grants.
	query := makeSQLQuery()
	// We use Sprintf instead of the more canonical query argument approach, as
	// that doesn't support arguments inside a SHOW subquery yet.
	query.Append(fmt.Sprintf("SELECT * FROM [SHOW GRANTS ON DATABASE %s]", escDBName))
	if limit > 0 {
		query.Append(" LIMIT $", limit)
		if offset > 0 {
			query.Append(" OFFSET $", offset)
		}
	}
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-grants", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		// We only want to show the grants on the database.
		query.String(), query.QueryArguments()...,
	)
	if err = s.maybeHandleNotFoundError(err); err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it sqlutil.InternalRows) {
		closeErr := it.Close()
		if retErr == nil {
			retErr = s.maybeHandleNotFoundError(closeErr)
		}
	}(it)
	{
		const (
			userCol       = "grantee"
			privilegesCol = "privilege_type"
		)

		ok, err := it.Next(ctx)
		if err = s.maybeHandleNotFoundError(err); err != nil {
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
			if err = s.maybeHandleNotFoundError(err); err != nil {
				return nil, err
			}
		}
	}
	return resp, retErr
}

func (s *adminServer) getDatabaseTables(
	ctx context.Context,
	req *serverpb.DatabaseDetailsRequest,
	userName security.SQLUsername,
	limit, offset int,
) (resp []string, retErr error) {
	query := makeSQLQuery()
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
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-tables", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName, Database: req.Database},
		query.String(), query.QueryArguments()...)
	if err = s.maybeHandleNotFoundError(err); err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it sqlutil.InternalRows) {
		closeErr := it.Close()
		if retErr == nil {
			retErr = s.maybeHandleNotFoundError(closeErr)
		}
	}(it)
	{
		ok, err := it.Next(ctx)
		if err = s.maybeHandleNotFoundError(err); err != nil {
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
			if err = s.maybeHandleNotFoundError(err); err != nil {
				return nil, err
			}
		}
	}
	return resp, retErr
}

func (s *adminServer) getMiscDatabaseDetails(
	ctx context.Context,
	req *serverpb.DatabaseDetailsRequest,
	userName security.SQLUsername,
	resp *serverpb.DatabaseDetailsResponse,
) (*serverpb.DatabaseDetailsResponse, error) {
	if resp == nil {
		resp = &serverpb.DatabaseDetailsResponse{}
	}
	// Query the descriptor ID and zone configuration for this database.
	databaseID, err := s.queryDatabaseID(ctx, userName, req.Database)
	if err != nil {
		return nil, s.serverError(err)
	}
	resp.DescriptorID = int64(databaseID)

	id, zone, zoneExists, err := s.queryZonePath(ctx, userName, []descpb.ID{databaseID})
	if err != nil {
		return nil, s.serverError(err)
	}

	if !zoneExists {
		zone = s.server.cfg.DefaultZoneConfig
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

func (s *adminServer) databaseDetailsHelper(
	ctx context.Context, req *serverpb.DatabaseDetailsRequest, userName security.SQLUsername,
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
	return &resp, nil
}

// getFullyQualifiedTableName, given a database name and a tableName that either
// is a unqualified name or a schema-qualified name, returns a maximally
// qualified name: either database.table if the input wasn't schema qualified,
// or database.schema.table if it was.
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
	ctx = s.server.AnnotateCtx(ctx)
	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	return s.tableDetailsHelper(ctx, req, userName)
}

func (s *adminServer) tableDetailsHelper(
	ctx context.Context, req *serverpb.TableDetailsRequest, userName security.SQLUsername,
) (_ *serverpb.TableDetailsResponse, retErr error) {
	escQualTable, err := getFullyQualifiedTableName(req.Database, req.Table)
	if err != nil {
		return nil, err
	}

	var resp serverpb.TableDetailsResponse

	// Marshal SHOW COLUMNS result.
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-columns",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW COLUMNS FROM %s", escQualTable),
	)
	if err = s.maybeHandleNotFoundError(err); err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it sqlutil.InternalRows) {
		closeErr := it.Close()
		if retErr == nil {
			retErr = s.maybeHandleNotFoundError(closeErr)
		}
	}(it)
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
		if err = s.maybeHandleNotFoundError(err); err != nil {
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
			if err = s.maybeHandleNotFoundError(err); err != nil {
				return nil, err
			}
		}
	}

	// Marshal SHOW INDEX result.
	it, err = s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-showindex", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW INDEX FROM %s", escQualTable),
	)
	if err = s.maybeHandleNotFoundError(err); err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it sqlutil.InternalRows) {
		closeErr := it.Close()
		if retErr == nil {
			retErr = s.maybeHandleNotFoundError(closeErr)
		}
	}(it)
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
		if err = s.maybeHandleNotFoundError(err); err != nil {
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
			if err = s.maybeHandleNotFoundError(err); err != nil {
				return nil, err
			}
		}
	}

	// Marshal SHOW GRANTS result.
	it, err = s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-show-grants", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW GRANTS ON TABLE %s", escQualTable),
	)
	if err = s.maybeHandleNotFoundError(err); err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it sqlutil.InternalRows) {
		closeErr := it.Close()
		if retErr == nil {
			retErr = s.maybeHandleNotFoundError(closeErr)
		}
	}(it)
	{
		const (
			userCol       = "grantee"
			privilegesCol = "privilege_type"
		)
		ok, err := it.Next(ctx)
		if err = s.maybeHandleNotFoundError(err); err != nil {
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
			if err = s.maybeHandleNotFoundError(err); err != nil {
				return nil, err
			}
		}
	}

	// Marshal SHOW CREATE result.
	row, cols, err := s.server.sqlServer.internalExecutor.QueryRowExWithCols(
		ctx, "admin-show-create", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW CREATE %s", escQualTable),
	)
	if err = s.maybeHandleNotFoundError(err); err != nil {
		return nil, err
	}
	{
		const createCol = "create_statement"
		if row == nil {
			return nil, s.serverErrorf("create response not available.")
		}

		scanner := makeResultScanner(cols)
		var createStmt string
		if err := scanner.Scan(row, createCol, &createStmt); err != nil {
			return nil, err
		}

		resp.CreateTableStatement = createStmt
	}

	// Marshal SHOW ZONE CONFIGURATION result.
	row, cols, err = s.server.sqlServer.internalExecutor.QueryRowExWithCols(
		ctx, "admin-show-zone-config", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		fmt.Sprintf("SHOW ZONE CONFIGURATION FOR TABLE %s", escQualTable))
	if err = s.maybeHandleNotFoundError(err); err != nil {
		return nil, err
	}
	{
		const rawConfigSQLColName = "raw_config_sql"
		if row == nil {
			return nil, s.serverErrorf("show zone config response not available.")
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
			return nil, s.serverError(err)
		}
		tableID, err = s.queryTableID(ctx, userName, req.Database, escQualTable)
		if err != nil {
			return nil, s.serverError(err)
		}
		resp.DescriptorID = int64(tableID)

		id, zone, zoneExists, err := s.queryZonePath(ctx, userName, []descpb.ID{databaseID, tableID})
		if err != nil {
			return nil, s.serverError(err)
		}

		if !zoneExists {
			zone = s.server.cfg.DefaultZoneConfig
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
		tableSpan := generateTableSpan(tableID)
		tableRSpan, err := keys.SpanAddr(tableSpan)
		if err != nil {
			return nil, s.serverError(err)
		}
		rangeCount, err := s.server.distSender.CountRanges(ctx, tableRSpan)
		if err != nil {
			return nil, s.serverError(err)
		}
		resp.RangeCount = rangeCount
	}

	return &resp, nil
}

// generateTableSpan generates a table's key span.
//
// NOTE: this doesn't make sense for interleaved (children) table. As of
// 03/2018, callers around here use it anyway.
func generateTableSpan(tableID descpb.ID) roachpb.Span {
	tableStartKey := keys.TODOSQLCodec.TablePrefix(uint32(tableID))
	tableEndKey := tableStartKey.PrefixEnd()
	return roachpb.Span{Key: tableStartKey, EndKey: tableEndKey}
}

// TableStats is an endpoint that returns disk usage and replication statistics
// for the specified table.
func (s *adminServer) TableStats(
	ctx context.Context, req *serverpb.TableStatsRequest,
) (*serverpb.TableStatsResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	userName, err := s.requireAdminUser(ctx)
	if err != nil {
		return nil, err
	}
	escQualTable, err := getFullyQualifiedTableName(req.Database, req.Table)
	if err != nil {
		return nil, err
	}

	tableID, err := s.queryTableID(ctx, userName, req.Database, escQualTable)
	if err != nil {
		return nil, s.serverError(err)
	}
	tableSpan := generateTableSpan(tableID)

	return s.statsForSpan(ctx, tableSpan)
}

// NonTableStats is an endpoint that returns disk usage and replication
// statistics for non-table parts of the system.
func (s *adminServer) NonTableStats(
	ctx context.Context, req *serverpb.NonTableStatsRequest,
) (*serverpb.NonTableStatsResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	if _, err := s.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	timeSeriesStats, err := s.statsForSpan(ctx, roachpb.Span{
		Key:    keys.TimeseriesPrefix,
		EndKey: keys.TimeseriesPrefix.PrefixEnd(),
	})
	if err != nil {
		return nil, err
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
	}
	for _, span := range spansForInternalUse {
		nonTableStats, err := s.statsForSpan(ctx, span)
		if err != nil {
			return nil, err
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

func (s *adminServer) statsForSpan(
	ctx context.Context, span roachpb.Span,
) (*serverpb.TableStatsResponse, error) {
	startKey, err := keys.Addr(span.Key)
	if err != nil {
		return nil, s.serverError(err)
	}
	endKey, err := keys.Addr(span.EndKey)
	if err != nil {
		return nil, s.serverError(err)
	}

	// Get current range descriptors for table. This is done by scanning over
	// meta2 keys for the range. A special case occurs if we wish to include
	// the meta1 key range itself, in which case we'll get KeyMin back and that
	// cannot be scanned (due to range-local addressing confusion). This is
	// handled appropriately by adjusting the bounds to grab the descriptors
	// for all ranges (including range1, which is not only gossiped but also
	// persisted in meta1).
	startMetaKey := keys.RangeMetaKey(startKey)
	if bytes.Equal(startMetaKey, roachpb.RKeyMin) {
		// This is the special case described above. The following key instructs
		// the code below to scan all of the addressing, i.e. grab all of the
		// descriptors including that for r1.
		startMetaKey = keys.RangeMetaKey(keys.MustAddr(keys.Meta2Prefix))
	}

	rangeDescKVs, err := s.server.db.Scan(ctx, startMetaKey, keys.RangeMetaKey(endKey), 0)
	if err != nil {
		return nil, s.serverError(err)
	}

	// This map will store the nodes we need to fan out to.
	nodeIDs := make(map[roachpb.NodeID]struct{})
	for _, kv := range rangeDescKVs {
		var rng roachpb.RangeDescriptor
		if err := kv.Value.GetProto(&rng); err != nil {
			return nil, s.serverError(err)
		}
		for _, repl := range rng.Replicas().Descriptors() {
			nodeIDs[repl.NodeID] = struct{}{}
		}
	}

	// Construct TableStatsResponse by sending an RPC to every node involved.
	tableStatResponse := serverpb.TableStatsResponse{
		NodeCount: int64(len(nodeIDs)),
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
		// See Github #5435 for some discussion.
		RangeCount: int64(len(rangeDescKVs)),
	}
	type nodeResponse struct {
		nodeID roachpb.NodeID
		resp   *serverpb.SpanStatsResponse
		err    error
	}

	// Send a SpanStats query to each node.
	responses := make(chan nodeResponse, len(nodeIDs))
	for nodeID := range nodeIDs {
		nodeID := nodeID // avoid data race
		if err := s.server.stopper.RunAsyncTask(
			ctx, "server.adminServer: requesting remote stats",
			func(ctx context.Context) {
				// Set a generous timeout on the context for each individual query.
				var spanResponse *serverpb.SpanStatsResponse
				err := contextutil.RunWithTimeout(ctx, "request remote stats", 5*base.NetworkTimeout,
					func(ctx context.Context) error {
						client, err := s.server.status.dialNode(ctx, nodeID)
						if err == nil {
							req := serverpb.SpanStatsRequest{
								StartKey: startKey,
								EndKey:   endKey,
								NodeID:   nodeID.String(),
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
				tableStatResponse.ApproximateDiskBytes += resp.resp.ApproximateDiskBytes
			}
		case <-ctx.Done():
			// Caller gave up, stop doing work.
			return nil, ctx.Err()
		}
	}

	return &tableStatResponse, nil
}

// Users returns a list of users, stripped of any passwords.
func (s *adminServer) Users(
	ctx context.Context, req *serverpb.UsersRequest,
) (*serverpb.UsersResponse, error) {
	ctx = s.server.AnnotateCtx(ctx)
	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}
	query := `SELECT username FROM system.users WHERE "isRole" = false`
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-users", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		query,
	)
	if err != nil {
		return nil, s.serverError(err)
	}

	var resp serverpb.UsersResponse
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		resp.Users = append(resp.Users, serverpb.UsersResponse_User{Username: string(tree.MustBeDString(row[0]))})
	}
	if err != nil {
		return nil, s.serverError(err)
	}
	return &resp, nil
}

var eventSetClusterSettingName = eventpb.GetEventTypeName(&eventpb.SetClusterSetting{})

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
	// All errors returned by this method must be serverErrors. We are careful
	// to not use serverError* methods in the body of the function, so we can
	// just do it here.
	defer func() {
		if retErr != nil {
			retErr = s.serverError(retErr)
		}
	}()

	ctx = s.server.AnnotateCtx(ctx)

	userName, err := s.requireAdminUser(ctx)
	if err != nil {
		return nil, err
	}
	redactEvents := !req.UnredactedEvents

	limit := req.Limit
	if limit == 0 {
		limit = defaultAPIEventLimit
	}

	return s.eventsHelper(ctx, req, userName, int(limit), 0, redactEvents)
}

func (s *adminServer) eventsHelper(
	ctx context.Context,
	req *serverpb.EventsRequest,
	userName security.SQLUsername,
	limit, offset int,
	redactEvents bool,
) (_ *serverpb.EventsResponse, retErr error) {
	// Execute the query.
	q := makeSQLQuery()
	q.Append(`SELECT timestamp, "eventType", "targetID", "reportingID", info, "uniqueID" `)
	q.Append("FROM system.eventlog ")
	q.Append("WHERE true ") // This simplifies the WHERE clause logic below.
	if len(req.Type) > 0 {
		q.Append(`AND "eventType" = $ `, req.Type)
	}
	if req.TargetId > 0 {
		q.Append(`AND "targetID" = $ `, req.TargetId)
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
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-events", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		q.String(), q.QueryArguments()...)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

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
		if err := scanner.ScanIndex(row, 2, &event.TargetID); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 3, &event.ReportingID); err != nil {
			return nil, err
		}
		if err := scanner.ScanIndex(row, 4, &event.Info); err != nil {
			return nil, err
		}
		if event.EventType == eventSetClusterSettingName {
			if redactEvents {
				event.Info = redactSettingsChange(event.Info)
			}
		}
		if err := scanner.ScanIndex(row, 5, &event.UniqueID); err != nil {
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
	// All errors returned by this method must be serverErrors. We are careful
	// to not use serverError* methods in the body of the function, so we can
	// just do it here.
	defer func() {
		if retErr != nil {
			retErr = s.serverError(retErr)
		}
	}()

	ctx = s.server.AnnotateCtx(ctx)

	// Range keys, even when pretty-printed, contain PII.
	userName, err := s.requireAdminUser(ctx)
	if err != nil {
		return nil, err
	}

	limit := req.Limit
	if limit == 0 {
		limit = defaultAPIEventLimit
	}

	// Execute the query.
	q := makeSQLQuery()
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
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-range-log", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		q.String(), q.QueryArguments()...,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

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
// responsibility to convert them to serverErrors.
func (s *adminServer) getUIData(
	ctx context.Context, userName security.SQLUsername, keys []string,
) (_ *serverpb.GetUIDataResponse, retErr error) {
	if len(keys) == 0 {
		return &serverpb.GetUIDataResponse{}, nil
	}

	// Query database.
	query := makeSQLQuery()
	query.Append(`SELECT key, value, "lastUpdated" FROM system.ui WHERE key IN (`)
	for i, key := range keys {
		if i != 0 {
			query.Append(",")
		}
		query.Append("$", tree.NewDString(makeUIKey(userName, key)))
	}
	query.Append(");")
	if err := query.Errors(); err != nil {
		return nil, errors.Errorf("error constructing query: %v", err)
	}
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-getUIData", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
func makeUIKey(username security.SQLUsername, key string) string {
	return username.Normalized() + "$" + key
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
	ctx = s.server.AnnotateCtx(ctx)

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if len(req.KeyValues) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "KeyValues cannot be empty")
	}

	for key, val := range req.KeyValues {
		// Do an upsert of the key. We update each key in a separate transaction to
		// avoid long-running transactions and possible deadlocks.
		query := `UPSERT INTO system.ui (key, value, "lastUpdated") VALUES ($1, $2, now())`
		rowsAffected, err := s.server.sqlServer.internalExecutor.ExecEx(
			ctx, "admin-set-ui-data", nil, /* txn */
			sessiondata.InternalExecutorOverride{
				User: security.RootUserName(),
			},
			query, makeUIKey(userName, key), val)
		if err != nil {
			return nil, s.serverError(err)
		}
		if rowsAffected != 1 {
			return nil, s.serverErrorf("rows affected %d != expected %d", rowsAffected, 1)
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
	ctx = s.server.AnnotateCtx(ctx)

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	if len(req.Keys) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "keys cannot be empty")
	}

	resp, err := s.getUIData(ctx, userName, req.Keys)
	if err != nil {
		return nil, s.serverError(err)
	}

	return resp, nil
}

// Settings returns settings associated with the given keys.
func (s *adminServer) Settings(
	ctx context.Context, req *serverpb.SettingsRequest,
) (*serverpb.SettingsResponse, error) {
	keys := req.Keys
	if len(keys) == 0 {
		keys = settings.Keys()
	}

	_, isAdmin, err := s.getUserAndRole(ctx)
	if err != nil {
		return nil, err
	}

	var lookupPurpose settings.LookupPurpose
	if isAdmin {
		// Root accesses can customize the purpose.
		// This is used by the UI to see all values (local access)
		// and `cockroach zip` to redact the values (telemetry).
		lookupPurpose = settings.LookupForReporting
		if req.UnredactedValues {
			lookupPurpose = settings.LookupForLocalAccess
		}
	} else {
		// Non-root access cannot see the values in any case.
		lookupPurpose = settings.LookupForReporting
	}

	resp := serverpb.SettingsResponse{KeyValues: make(map[string]serverpb.SettingsResponse_Value)}
	for _, k := range keys {
		v, ok := settings.Lookup(k, lookupPurpose)
		if !ok {
			continue
		}
		resp.KeyValues[k] = serverpb.SettingsResponse_Value{
			Type: v.Typ(),
			// Note: v.String() redacts the values if the purpose is not "LocalAccess".
			Value:       v.String(&s.server.st.SV),
			Description: v.Description(),
			Public:      v.Visibility() == settings.Public,
		}
	}

	return &resp, nil
}

// Cluster returns cluster metadata.
func (s *adminServer) Cluster(
	_ context.Context, req *serverpb.ClusterRequest,
) (*serverpb.ClusterResponse, error) {
	clusterID := s.server.ClusterID()
	if clusterID == (uuid.UUID{}) {
		return nil, status.Errorf(codes.Unavailable, "cluster ID not yet available")
	}

	// Check if enterprise features are enabled.  We currently test for the
	// feature "BACKUP", although enterprise licenses do not yet distinguish
	// between different features.
	organization := sql.ClusterOrganization.Get(&s.server.st.SV)
	enterpriseEnabled := base.CheckEnterpriseEnabled(s.server.st, clusterID, organization, "BACKUP") == nil

	return &serverpb.ClusterResponse{
		ClusterID:         clusterID.String(),
		ReportingEnabled:  logcrash.DiagnosticsReportingEnabled.Get(&s.server.st.SV),
		EnterpriseEnabled: enterpriseEnabled,
	}, nil
}

// Health returns liveness for the node target of the request.
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

func (s *adminServer) checkReadinessForHealthCheck(ctx context.Context) error {
	serveMode := s.server.grpc.mode.get()
	switch serveMode {
	case modeInitializing:
		return status.Error(codes.Unavailable, "node is waiting for cluster initialization")
	case modeDraining:
		// grpc.mode is set to modeDraining when the Drain(DrainMode_CLIENT) has
		// been called (client connections are to be drained).
		return status.Errorf(codes.Unavailable, "node is shutting down")
	case modeOperational:
		break
	default:
		return s.serverError(errors.Newf("unknown mode: %v", serveMode))
	}

	// TODO(knz): update this code when progress is made on
	// https://github.com/cockroachdb/cockroach/issues/45123
	l, ok := s.server.nodeLiveness.GetLiveness(s.server.NodeID())
	if !ok {
		return status.Error(codes.Unavailable, "liveness record not found")
	}
	if !l.IsLive(s.server.clock.Now().GoTime()) {
		return status.Errorf(codes.Unavailable, "node is not healthy")
	}
	if l.Draining {
		// l.Draining indicates that the node is draining leases.
		// This is set when Drain(DrainMode_LEASES) is called.
		// It's possible that l.Draining is set without
		// grpc.mode being modeDraining, if a RPC client
		// has requested DrainMode_LEASES but not DrainMode_CLIENT.
		return status.Errorf(codes.Unavailable, "node is shutting down")
	}

	if !s.server.sqlServer.acceptingClients.Get() {
		return status.Errorf(codes.Unavailable, "node is not accepting SQL clients")
	}

	return nil
}

// getLivenessStatusMap generates a map from NodeID to LivenessStatus for all
// nodes known to gossip. Nodes that haven't pinged their liveness record for
// more than server.time_until_store_dead are considered dead.
//
// To include all nodes (including ones not in the gossip network), callers
// should consider calling (statusServer).NodesWithLiveness() instead where
// possible.
//
// getLivenessStatusMap() includes removed nodes (dead + decommissioned).
func getLivenessStatusMap(
	nl *liveness.NodeLiveness, now time.Time, st *cluster.Settings,
) map[roachpb.NodeID]livenesspb.NodeLivenessStatus {
	livenesses := nl.GetLivenesses()
	threshold := kvserver.TimeUntilStoreDead.Get(&st.SV)

	statusMap := make(map[roachpb.NodeID]livenesspb.NodeLivenessStatus, len(livenesses))
	for _, liveness := range livenesses {
		status := kvserver.LivenessStatus(liveness, now, threshold)
		statusMap[liveness.NodeID] = status
	}
	return statusMap
}

// Liveness returns the liveness state of all nodes on the cluster
// known to gossip. To reach all nodes in the cluster, consider
// using (statusServer).NodesWithLiveness instead.
func (s *adminServer) Liveness(
	context.Context, *serverpb.LivenessRequest,
) (*serverpb.LivenessResponse, error) {
	clock := s.server.clock
	statusMap := getLivenessStatusMap(
		s.server.nodeLiveness, clock.Now().GoTime(), s.server.st)
	livenesses := s.server.nodeLiveness.GetLivenesses()
	return &serverpb.LivenessResponse{
		Livenesses: livenesses,
		Statuses:   statusMap,
	}, nil
}

func (s *adminServer) Jobs(
	ctx context.Context, req *serverpb.JobsRequest,
) (_ *serverpb.JobsResponse, retErr error) {
	// All errors returned by this method must be serverErrors. We are careful
	// to not use serverError* methods in the body of the function, so we can
	// just do it here.
	defer func() {
		if retErr != nil {
			retErr = s.serverError(retErr)
		}
	}()

	ctx = s.server.AnnotateCtx(ctx)

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	q := makeSQLQuery()
	q.Append(`
      SELECT job_id, job_type, description, statement, user_name, descriptor_ids, status,
						 running_status, created, started, finished, modified,
						 fraction_completed, high_water_timestamp, error
        FROM crdb_internal.jobs
       WHERE true
	`)
	if req.Status != "" {
		q.Append(" AND status = $", req.Status)
	}
	if req.Type != jobspb.TypeUnspecified {
		q.Append(" AND job_type = $", req.Type.String())
	} else {
		// Don't show auto stats jobs in the overview page.
		q.Append(" AND (job_type != $ OR job_type IS NULL)", jobspb.TypeAutoCreateStats.String())
	}
	q.Append("ORDER BY created DESC")
	if req.Limit > 0 {
		q.Append(" LIMIT $", tree.DInt(req.Limit))
	}
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-jobs", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		q.String(), q.QueryArguments()...,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

	ok, err := it.Next(ctx)
	if err != nil {
		return nil, err
	}

	var resp serverpb.JobsResponse
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
	if err := scanner.ScanAll(
		row,
		&job.ID,
		&job.Type,
		&job.Description,
		&job.Statement,
		&job.Username,
		&job.DescriptorIDs,
		&job.Status,
		&runningStatusOrNil,
		&job.Created,
		&job.Started,
		&job.Finished,
		&job.Modified,
		&fractionCompletedOrNil,
		&highwaterOrNil,
		&job.Error,
	); err != nil {
		return err
	}
	if highwaterOrNil != nil {
		highwaterTimestamp, err := tree.DecimalToHLC(highwaterOrNil)
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
	return nil
}

func (s *adminServer) Job(
	ctx context.Context, request *serverpb.JobRequest,
) (_ *serverpb.JobResponse, retErr error) {
	// All errors returned by this method must be serverErrors. We are careful
	// to not use serverError* methods in the body of the function, so we can
	// just do it here.
	defer func() {
		if retErr != nil {
			retErr = s.serverError(retErr)
		}
	}()

	ctx = s.server.AnnotateCtx(ctx)

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	const query = `
      SELECT job_id, job_type, description, statement, user_name, descriptor_ids, status,
						 running_status, created, started, finished, modified,
						 fraction_completed, high_water_timestamp, error
        FROM crdb_internal.jobs
       WHERE job_id = $1`

	row, cols, err := s.server.sqlServer.internalExecutor.QueryRowExWithCols(
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

	return &job, nil
}

func (s *adminServer) Locations(
	ctx context.Context, req *serverpb.LocationsRequest,
) (_ *serverpb.LocationsResponse, retErr error) {
	// All errors returned by this method must be serverErrors. We are careful
	// to not use serverError* methods in the body of the function, so we can
	// just do it here.
	defer func() {
		if retErr != nil {
			retErr = s.serverError(retErr)
		}
	}()

	ctx = s.server.AnnotateCtx(ctx)

	_, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	q := makeSQLQuery()
	q.Append(`SELECT "localityKey", "localityValue", latitude, longitude FROM system.locations`)
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-locations", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		q.String(),
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func() { retErr = errors.CombineErrors(retErr, it.Close()) }()

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
	ctx = s.server.AnnotateCtx(ctx)

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// As long as there's only one query provided it's safe to construct the
	// explain query.
	stmts, err := parser.Parse(req.Query)
	if err != nil {
		return nil, s.serverError(err)
	}
	if len(stmts) > 1 {
		return nil, s.serverErrorf("more than one query provided")
	}

	explain := fmt.Sprintf(
		"EXPLAIN (DISTSQL, JSON) %s",
		strings.Trim(req.Query, ";"))
	row, err := s.server.sqlServer.internalExecutor.QueryRowEx(
		ctx, "admin-query-plan", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		explain,
	)
	if err != nil {
		return nil, s.serverError(err)
	}
	if row == nil {
		return nil, s.serverError(errors.New("failed to query the physical plan"))
	}

	dbDatum, ok := tree.AsDString(row[0])
	if !ok {
		return nil, s.serverErrorf("type assertion failed on json: %T", row)
	}

	return &serverpb.QueryPlanResponse{
		DistSQLPhysicalQueryPlan: string(dbDatum),
	}, nil
}

// getStatementBundle retrieves the statement bundle with the given id and
// writes it out as an attachment.
func (s *adminServer) getStatementBundle(ctx context.Context, id int64, w http.ResponseWriter) {
	sessionUser, err := userFromContext(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	row, err := s.server.sqlServer.internalExecutor.QueryRowEx(
		ctx, "admin-stmt-bundle", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: sessionUser},
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
		chunkRow, err := s.server.sqlServer.internalExecutor.QueryRowEx(
			ctx, "admin-stmt-bundle", nil, /* txn */
			sessiondata.InternalExecutorOverride{User: sessionUser},
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

// DecommissionStatus returns the DecommissionStatus for all or the given nodes.
func (s *adminServer) DecommissionStatus(
	ctx context.Context, req *serverpb.DecommissionStatusRequest,
) (*serverpb.DecommissionStatusResponse, error) {
	// Get the number of replicas on each node. We *may* not need all of them,
	// but that would be more complicated than seems worth it right now.
	nodeIDs := req.NodeIDs

	// If no nodeIDs given, use all nodes.
	if len(nodeIDs) == 0 {
		ns, err := s.server.status.Nodes(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return nil, errors.Wrap(err, "loading node statuses")
		}
		for _, status := range ns.Nodes {
			nodeIDs = append(nodeIDs, status.Desc.NodeID)
		}
	}

	// Compute the replica counts for the target nodes only. This map doubles as
	// a lookup table to check whether we care about a given node.
	var replicaCounts map[roachpb.NodeID]int64
	if err := s.server.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
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

	for nodeID := range replicaCounts {
		l, ok := s.server.nodeLiveness.GetLiveness(nodeID)
		if !ok {
			return nil, errors.Newf("unable to get liveness for %d", nodeID)
		}
		nodeResp := serverpb.DecommissionStatusResponse_Status{
			NodeID:       l.NodeID,
			ReplicaCount: replicaCounts[l.NodeID],
			Membership:   l.Membership,
			Draining:     l.Draining,
		}
		if l.IsLive(s.server.clock.Now().GoTime()) {
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
func (s *adminServer) Decommission(
	ctx context.Context, req *serverpb.DecommissionRequest,
) (*serverpb.DecommissionStatusResponse, error) {
	nodeIDs := req.NodeIDs

	if len(nodeIDs) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "no node ID specified")
	}

	// Mark the target nodes with their new membership status. They'll find out
	// as they heartbeat their liveness.
	if err := s.server.Decommission(ctx, req.TargetMembership, nodeIDs); err != nil {
		return nil, err
	}

	// We return an empty response when setting the final DECOMMISSIONED state,
	// since a node can be asked to decommission itself which may cause it to
	// lose access to cluster RPC and fail to populate the response.
	if req.TargetMembership == livenesspb.MembershipStatus_DECOMMISSIONED {
		return &serverpb.DecommissionStatusResponse{}, nil
	}

	return s.DecommissionStatus(ctx, &serverpb.DecommissionStatusRequest{NodeIDs: nodeIDs})
}

// DataDistribution returns a count of replicas on each node for each table.
func (s *adminServer) DataDistribution(
	ctx context.Context, req *serverpb.DataDistributionRequest,
) (_ *serverpb.DataDistributionResponse, retErr error) {
	// All errors returned by this method must be serverErrors. We are careful
	// to not use serverError* methods in the body of the function, so we can
	// just do it here.
	defer func() {
		if retErr != nil {
			retErr = s.serverError(retErr)
		}
	}()

	if _, err := s.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	resp := &serverpb.DataDistributionResponse{
		DatabaseInfo: make(map[string]serverpb.DataDistributionResponse_DatabaseInfo),
		ZoneConfigs:  make(map[string]serverpb.DataDistributionResponse_ZoneConfig),
	}

	userName, err := userFromContext(ctx)
	if err != nil {
		return nil, err
	}

	// Get ids and names for databases and tables.
	// Set up this structure in the response.

	// This relies on crdb_internal.tables returning data even for newly added tables
	// and deleted tables (as opposed to e.g. information_schema) because we are interested
	// in the data for all ranges, not just ranges for visible tables.
	//
	// Don't include tables with a NULL database_name, which in this case means
	// excluding virtual tables (like crdb_internal.tables itself, for example).
	tablesQuery := `SELECT name, schema_name, table_id, database_name, drop_time FROM
									"".crdb_internal.tables WHERE database_name IS NOT NULL`
	it, err := s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-replica-matrix", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		tablesQuery,
	)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it sqlutil.InternalRows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	// Used later when we're scanning Meta2 and only have IDs, not names.
	tableInfosByTableID := map[uint32]serverpb.DataDistributionResponse_TableInfo{}

	var hasNext bool
	for hasNext, err = it.Next(ctx); hasNext; hasNext, err = it.Next(ctx) {
		row := it.Cur()
		tableName := (*string)(row[0].(*tree.DString))
		schemaName := (*string)(row[1].(*tree.DString))
		fqTableName := fmt.Sprintf("%s.%s",
			tree.NameStringP(schemaName), tree.NameStringP(tableName))
		tableID := uint32(tree.MustBeDInt(row[2]))
		dbName := (*string)(row[3].(*tree.DString))

		// Look at whether it was dropped.
		var droppedAtTime *time.Time
		droppedAtDatum, ok := row[4].(*tree.DTimestamp)
		if ok {
			droppedAtTime = &droppedAtDatum.Time
		}

		// Insert database if it doesn't exist.
		dbInfo, ok := resp.DatabaseInfo[*dbName]
		if !ok {
			dbInfo = serverpb.DataDistributionResponse_DatabaseInfo{
				TableInfo: make(map[string]serverpb.DataDistributionResponse_TableInfo),
			}
			resp.DatabaseInfo[*dbName] = dbInfo
		}

		// Get zone config for table.
		zcID := int64(0)

		if droppedAtTime == nil {
			// TODO(vilterp): figure out a way to get zone configs for tables that are dropped
			zoneConfigQuery := fmt.Sprintf(
				`SELECT zone_id FROM [SHOW ZONE CONFIGURATION FOR TABLE %s.%s.%s]`,
				(*tree.Name)(dbName), (*tree.Name)(schemaName), (*tree.Name)(tableName),
			)
			row, err := s.server.sqlServer.internalExecutor.QueryRowEx(
				ctx, "admin-replica-matrix", nil, /* txn */
				sessiondata.InternalExecutorOverride{User: userName},
				zoneConfigQuery,
			)
			if err != nil {
				return nil, err
			}
			if row == nil {
				return nil, errors.Errorf(
					"could not get zone config for table %s; 0 rows returned", *tableName,
				)
			}

			zcID = int64(tree.MustBeDInt(row[0]))
		}

		// Insert table.
		tableInfo := serverpb.DataDistributionResponse_TableInfo{
			ReplicaCountByNodeId: make(map[roachpb.NodeID]int64),
			ZoneConfigId:         zcID,
			DroppedAt:            droppedAtTime,
		}
		dbInfo.TableInfo[fqTableName] = tableInfo
		tableInfosByTableID[tableID] = tableInfo
	}
	if err != nil {
		return nil, err
	}

	// Get replica counts.
	if err := s.server.db.Txn(ctx, func(txnCtx context.Context, txn *kv.Txn) error {
		acct := s.memMonitor.MakeBoundAccount()
		defer acct.Close(txnCtx)

		kvs, err := kvclient.ScanMetaKVs(ctx, txn, roachpb.Span{
			Key:    keys.UserTableDataMin,
			EndKey: keys.MaxKey,
		})
		if err != nil {
			return err
		}

		// Group replicas by table and node, accumulate counts.
		var rangeDesc roachpb.RangeDescriptor
		for _, kv := range kvs {
			if err := acct.Grow(txnCtx, int64(len(kv.Key)+len(kv.Value.RawBytes))); err != nil {
				return err
			}
			if err := kv.ValueProto(&rangeDesc); err != nil {
				return err
			}

			_, tableID, err := keys.TODOSQLCodec.DecodeTablePrefix(rangeDesc.StartKey.AsRawKey())
			if err != nil {
				return err
			}

			for _, replicaDesc := range rangeDesc.Replicas().Descriptors() {
				tableInfo, found := tableInfosByTableID[tableID]
				if !found {
					// This is a database, skip.
					continue
				}
				tableInfo.ReplicaCountByNodeId[replicaDesc.NodeID]++
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Get zone configs.
	// TODO(vilterp): this can be done in parallel with getting table/db names and replica counts.
	zoneConfigsQuery := `
		SELECT target, raw_config_sql, raw_config_protobuf
		FROM crdb_internal.zones
		WHERE target IS NOT NULL
	`
	it, err = s.server.sqlServer.internalExecutor.QueryIteratorEx(
		ctx, "admin-replica-matrix", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: userName},
		zoneConfigsQuery)
	if err != nil {
		return nil, err
	}
	// We have to make sure to close the iterator since we might return from the
	// for loop early (before Next() returns false).
	defer func(it sqlutil.InternalRows) { retErr = errors.CombineErrors(retErr, it.Close()) }(it)

	for hasNext, err = it.Next(ctx); hasNext; hasNext, err = it.Next(ctx) {
		row := it.Cur()
		target := string(tree.MustBeDString(row[0]))
		zcSQL := tree.MustBeDString(row[1])
		zcBytes := tree.MustBeDBytes(row[2])
		var zcProto zonepb.ZoneConfig
		if err := protoutil.Unmarshal([]byte(zcBytes), &zcProto); err != nil {
			return nil, err
		}

		resp.ZoneConfigs[target] = serverpb.DataDistributionResponse_ZoneConfig{
			Target:    target,
			Config:    zcProto,
			ConfigSQL: string(zcSQL),
		}
	}
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// EnqueueRange runs the specified range through the specified queue, returning
// the detailed trace and error information from doing so.
func (s *adminServer) EnqueueRange(
	ctx context.Context, req *serverpb.EnqueueRangeRequest,
) (*serverpb.EnqueueRangeResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.server.AnnotateCtx(ctx)

	if _, err := s.requireAdminUser(ctx); err != nil {
		return nil, err
	}

	if req.NodeID < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "node_id must be non-negative; got %d", req.NodeID)
	}
	if req.Queue == "" {
		return nil, status.Errorf(codes.InvalidArgument, "queue name must be non-empty")
	}
	if req.RangeID <= 0 {
		return nil, status.Errorf(codes.InvalidArgument, "range_id must be positive; got %d", req.RangeID)
	}

	// If the request is targeted at this node, serve it directly. Otherwise,
	// forward it to the appropriate node(s). If no node was specified, forward
	// it to all nodes.
	if req.NodeID == s.server.NodeID() {
		return s.enqueueRangeLocal(ctx, req)
	} else if req.NodeID != 0 {
		admin, err := s.dialNode(ctx, req.NodeID)
		if err != nil {
			return nil, err
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

	if err := contextutil.RunWithTimeout(ctx, "enqueue range", time.Minute, func(ctx context.Context) error {
		return s.server.status.iterateNodes(
			ctx, fmt.Sprintf("enqueue r%d in queue %s", req.RangeID, req.Queue),
			dialFn, nodeFn, responseFn, errorFn,
		)
	}); err != nil {
		if len(response.Details) == 0 {
			return nil, err
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
func (s *adminServer) enqueueRangeLocal(
	ctx context.Context, req *serverpb.EnqueueRangeRequest,
) (*serverpb.EnqueueRangeResponse, error) {
	response := &serverpb.EnqueueRangeResponse{
		Details: []*serverpb.EnqueueRangeResponse_Details{
			{
				NodeID: s.server.NodeID(),
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

	traceSpans, processErr, err := store.ManuallyEnqueue(ctx, req.Queue, repl, req.SkipShouldQueue)
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

// sqlQuery allows you to incrementally build a SQL query that uses
// placeholders. Instead of specific placeholders like $1, you instead use the
// temporary placeholder $.
type sqlQuery struct {
	buf   bytes.Buffer
	pidx  int
	qargs []interface{}
	errs  []error
}

func makeSQLQuery() *sqlQuery {
	res := &sqlQuery{}
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
func (q *sqlQuery) QueryArguments() []interface{} {
	return q.qargs
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
func (q *sqlQuery) Append(s string, params ...interface{}) {
	var placeholders int
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
	q.qargs = append(q.qargs, params...)
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
			return errors.Errorf("source type assertion failed")
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
		s, ok := src.(*tree.DTimestamp)
		if !ok {
			return errors.Errorf("source type assertion failed")
		}
		*d = s.Time

	// Passing a **time.Time instead of a *time.Time means the source is allowed
	// to be NULL, in which case nil is stored into *src.
	case **time.Time:
		s, ok := src.(*tree.DTimestamp)
		if !ok {
			if src != tree.DNull {
				return errors.Errorf("source type assertion failed")
			}
			*d = nil
			break
		}
		*d = &s.Time

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
// server with a SQL connections, while this code uses the InternalExecutor, the
// code cannot be commonized.
//
// queryZone retrieves the specific ZoneConfig associated with the supplied ID,
// if it exists.
func (s *adminServer) queryZone(
	ctx context.Context, userName security.SQLUsername, id descpb.ID,
) (zonepb.ZoneConfig, bool, error) {
	const query = `SELECT crdb_internal.get_zone_config($1)`
	row, cols, err := s.server.sqlServer.internalExecutor.QueryRowExWithCols(
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
func (s *adminServer) queryZonePath(
	ctx context.Context, userName security.SQLUsername, path []descpb.ID,
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
func (s *adminServer) queryDatabaseID(
	ctx context.Context, userName security.SQLUsername, name string,
) (descpb.ID, error) {
	const query = `SELECT crdb_internal.get_database_id($1)`
	row, cols, err := s.server.sqlServer.internalExecutor.QueryRowExWithCols(
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
func (s *adminServer) queryTableID(
	ctx context.Context, username security.SQLUsername, database string, tableName string,
) (descpb.ID, error) {
	row, err := s.server.sqlServer.internalExecutor.QueryRowEx(
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
	return descpb.ID(tree.MustBeDOid(row[0]).DInt), nil
}

func (s *adminServer) dialNode(
	ctx context.Context, nodeID roachpb.NodeID,
) (serverpb.AdminClient, error) {
	addr, err := s.server.gossip.GetNodeIDAddress(nodeID)
	if err != nil {
		return nil, err
	}
	conn, err := s.server.rpcContext.GRPCDialNode(
		addr.String(), nodeID, rpc.DefaultClass).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return serverpb.NewAdminClient(conn), nil
}

// adminPrivilegeChecker is a helper struct to check whether given usernames
// have admin privileges.
type adminPrivilegeChecker struct {
	ie *sql.InternalExecutor
}

func (c *adminPrivilegeChecker) requireAdminUser(
	ctx context.Context,
) (userName security.SQLUsername, err error) {
	userName, isAdmin, err := c.getUserAndRole(ctx)
	if err != nil {
		return userName, err
	}
	if !isAdmin {
		return userName, errRequiresAdmin
	}
	return userName, nil
}

func (c *adminPrivilegeChecker) requireViewActivityPermission(
	ctx context.Context,
) (userName security.SQLUsername, err error) {
	userName, isAdmin, err := c.getUserAndRole(ctx)
	if err != nil {
		return userName, err
	}
	if !isAdmin {
		hasViewActivity, err := c.hasRoleOption(ctx, userName, roleoption.VIEWACTIVITY)
		if err != nil {
			return userName, err
		}
		if !hasViewActivity {
			return userName, status.Errorf(
				codes.PermissionDenied, "this operation requires the %s role option",
				roleoption.VIEWACTIVITY)
		}
	}
	return userName, nil
}

func (c *adminPrivilegeChecker) getUserAndRole(
	ctx context.Context,
) (userName security.SQLUsername, isAdmin bool, err error) {
	userName, err = userFromContext(ctx)
	if err != nil {
		return userName, false, err
	}
	isAdmin, err = c.hasAdminRole(ctx, userName)
	return userName, isAdmin, err
}

func (c *adminPrivilegeChecker) hasAdminRole(
	ctx context.Context, user security.SQLUsername,
) (bool, error) {
	if user.IsRootUser() {
		// Shortcut.
		return true, nil
	}
	row, err := c.ie.QueryRowEx(
		ctx, "check-is-admin", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: user},
		"SELECT crdb_internal.is_admin()")
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, errors.AssertionFailedf("hasAdminRole: expected 1 row, got 0")
	}
	if len(row) != 1 {
		return false, errors.AssertionFailedf("hasAdminRole: expected 1 column, got %d", len(row))
	}
	dbDatum, ok := tree.AsDBool(row[0])
	if !ok {
		return false, errors.AssertionFailedf("hasAdminRole: expected bool, got %T", row[0])
	}
	return bool(dbDatum), nil
}

func (c *adminPrivilegeChecker) hasRoleOption(
	ctx context.Context, user security.SQLUsername, roleOption roleoption.Option,
) (bool, error) {
	if user.IsRootUser() {
		// Shortcut.
		return true, nil
	}
	row, err := c.ie.QueryRowEx(
		ctx, "check-role-option", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: user},
		"SELECT crdb_internal.has_role_option($1)", roleOption.String())
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, errors.AssertionFailedf("hasRoleOption: expected 1 row, got 0")
	}
	if len(row) != 1 {
		return false, errors.AssertionFailedf("hasRoleOption: expected 1 column, got %d", len(row))
	}
	dbDatum, ok := tree.AsDBool(row[0])
	if !ok {
		return false, errors.AssertionFailedf("hasRoleOption: expected bool, got %T", row[0])
	}
	return bool(dbDatum), nil
}

var errRequiresAdmin = status.Error(codes.PermissionDenied, "this operation requires admin privilege")

func errRequiresRoleOption(option roleoption.Option) error {
	return status.Errorf(
		codes.PermissionDenied, "this operation requires %s privilege", option)
}
