// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/spanutils"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

type SelectQueryParams struct {
	RelationName      string
	PKColNames        []string
	PKColDirs         []catenumpb.IndexColumn_Direction
	PKColTypes        []*types.T
	Bounds            spanutils.QueryBounds
	AOSTDuration      time.Duration
	SelectBatchSize   int64
	TTLExpr           catpb.Expression
	SelectDuration    *aggmetric.Histogram
	SelectRateLimiter *quotapool.RateLimiter
	// User and DescriptorOverrides identify the session the SELECT statements
	// run as; see ttlSessionOverrides. The map is captured by reference and
	// must not be mutated after construction.
	User                username.SQLUsername
	DescriptorOverrides map[uint32]sessiondata.DescriptorOverride
}

type SelectQueryBuilder interface {
	// Run will perform the SELECT operation and return the rows.
	Run(ctx context.Context, ie isql.Executor) (_ []tree.Datums, hasNext bool, _ error)

	// BuildQuery will generate the SELECT query for the given builder.
	BuildQuery() (string, error)
}

// SelectQueryBuilder is responsible for maintaining state around the SELECT
// portion of the TTL job.
type selectQueryBuilder struct {
	SelectQueryParams
	selectOpName redact.RedactableString
	// isFirst is true if we have not invoked a query using the builder yet.
	isFirst bool
	// cachedQuery is the cached query, which stays the same from the second
	// iteration onwards.
	cachedQuery string
	// cachedArgs keeps a cache of args to use in the run query.
	// The cache is of form [cutoff, <endFilterClause...>, <startFilterClause..>].
	cachedArgs []interface{}
}

func MakeSelectQueryBuilder(
	params SelectQueryParams, cutoff time.Time,
) (SelectQueryBuilder, error) {
	numPkCols := len(params.PKColNames)
	if numPkCols == 0 {
		return nil, errors.AssertionFailedf("PKColNames is empty")
	}
	// An unset User would silently fall back to the internal executor's
	// default identity (the node user, which bypasses privilege checks).
	if params.User.Undefined() {
		return nil, errors.AssertionFailedf("User is unset")
	}
	if numPkCols != len(params.PKColDirs) {
		return nil, errors.AssertionFailedf("different number of PKColNames and PKColDirs")
	}
	// We will have a maximum of 1 + len(PKColNames)*2 columns, where one
	// is reserved for AOST, and len(PKColNames) for both start and end key.
	cachedArgs := make([]interface{}, 0, 1+numPkCols*2)
	cachedArgs = append(cachedArgs, cutoff)
	for _, d := range params.Bounds.End {
		cachedArgs = append(cachedArgs, d)
	}
	for _, d := range params.Bounds.Start {
		cachedArgs = append(cachedArgs, d)
	}

	return &selectQueryBuilder{
		SelectQueryParams: params,
		selectOpName:      redact.Sprintf("ttl select %s", params.RelationName),
		cachedArgs:        cachedArgs,
		isFirst:           true,
	}, nil
}

// BuildQuery implements the SelectQueryBuilder interface.
func (b *selectQueryBuilder) BuildQuery() (string, error) {
	return ttlbase.BuildSelectQuery(
		b.RelationName,
		b.PKColNames,
		b.PKColDirs,
		b.PKColTypes,
		b.AOSTDuration,
		b.TTLExpr,
		len(b.Bounds.Start),
		len(b.Bounds.End),
		b.SelectBatchSize,
		b.isFirst,
	)
}

// ttlSessionOverrides returns the identity that TTL statements against the given
// table run as — the table's owner — together with the descriptor overrides that
// identity needs.
//
// Running the SELECT, DELETE, and row-count statements as the owner rather than
// the node user (which bypasses every privilege check) ensures that any
// user-defined code they reach — DELETE triggers on the TTL table, and the
// CHECK/computed-column UDFs re-evaluated by foreign-key cascades — executes
// with the owner's privileges rather than node's. TTL itself never calls
// crdb_internal.execute_internally, but a trigger fired by the TTL delete can;
// under node's identity that would run arbitrary SQL with node's authority,
// while under the owner's it is stopped by the builtin's global REPAIRCLUSTER
// requirement (see TestRowLevelTTLCheckConstraintNoEscalation).
//
// Foreign-key referential actions triggered by the delete authorize their own
// structural access as the referencing (child) table's owner, so the owner
// running the TTL delete needs no privileges on the foreign-key cascade closure.
// The overrides are therefore scoped to the TTL table itself and what naming and
// planning it requires — access the node user was silently satisfying, which the
// owner may lack:
//
//   - a row-level-security bypass on the TTL table, so a FORCE ROW LEVEL
//     SECURITY policy does not hide expired rows from the owner and leave them
//     undeleted (the owner already holds DML on the table it owns);
//   - USAGE/CONNECT on the containing schema and database, for resolving the
//     table's fully-qualified name; and
//   - EXECUTE on the functions the table references (computed columns, CHECK and
//     DEFAULT expressions, the TTL expression, partial-index predicates), which
//     are built when the table is scanned.
//
// A DescriptorOverride can only grant per-descriptor privileges, never a global
// privilege such as REPAIRCLUSTER, so these grants do not reopen the escalation.
func ttlSessionOverrides(
	tbl catalog.TableDescriptor,
) (username.SQLUsername, map[uint32]sessiondata.DescriptorOverride, error) {
	// Every valid descriptor has an owner (catprivilege.MaybeFixPrivileges
	// backfills one on read), but an undefined owner passed to the internal
	// executor would silently run the TTL statements as the privilege-bypassing
	// node user, so fail loudly rather than fall through.
	owner := tbl.GetPrivileges().Owner()
	if owner.Undefined() {
		return username.SQLUsername{}, nil, errors.AssertionFailedf(
			"TTL table %d has no owner to run TTL statements as", tbl.GetID())
	}
	overrides := map[uint32]sessiondata.DescriptorOverride{
		uint32(tbl.GetID()):             {BypassRLS: true},
		uint32(tbl.GetParentID()):       {Privileges: privilege.List{privilege.CONNECT}.ToBitField()},
		uint32(tbl.GetParentSchemaID()): {Privileges: privilege.List{privilege.USAGE}.ToBitField()},
	}
	fnIDs, err := tbl.GetAllReferencedFunctionIDs()
	if err != nil {
		return username.SQLUsername{}, nil, errors.Wrap(err, "collecting TTL descriptor overrides")
	}
	execPriv := privilege.List{privilege.EXECUTE}.ToBitField()
	for _, id := range fnIDs.Ordered() {
		overrides[uint32(id)] = sessiondata.DescriptorOverride{Privileges: execPriv}
	}
	return owner, overrides, nil
}

func getInternalExecutorOverride(
	user username.SQLUsername,
	descriptorOverrides map[uint32]sessiondata.DescriptorOverride,
	qosLevel sessiondatapb.QoSLevel,
) sessiondata.InternalExecutorOverride {
	return sessiondata.InternalExecutorOverride{
		User:                   user,
		DescriptorOverrides:    descriptorOverrides,
		QualityOfService:       &qosLevel,
		OptimizerUseHistograms: true,
	}
}

// Run implements the SelectQueryBuilder interface.
func (b *selectQueryBuilder) Run(
	ctx context.Context, ie isql.Executor,
) (_ []tree.Datums, hasNext bool, _ error) {
	var query string
	var err error
	if b.isFirst {
		query, err = b.BuildQuery()
		if err != nil {
			return nil, false, err
		}
		b.isFirst = false
	} else {
		if b.cachedQuery == "" {
			b.cachedQuery, err = b.BuildQuery()
			if err != nil {
				return nil, false, err
			}
		}
		query = b.cachedQuery
	}
	// Convert any DEnum args to their logical representation to avoid the risk
	// of using the wrong version of the enum type descriptor.
	for i, arg := range b.cachedArgs {
		if enum, ok := arg.(*tree.DEnum); ok {
			b.cachedArgs[i] = enum.LogicalRep
		}
	}

	tokens, err := b.SelectRateLimiter.Acquire(ctx, b.SelectBatchSize)
	if err != nil {
		return nil, false, err
	}
	defer tokens.Consume()

	start := timeutil.Now()
	// Use a nil txn so that the AOST clause is handled correctly. Currently,
	// the internal executor will treat a passed-in txn as an explicit txn, so
	// the AOST clause on the SELECT query would not be interpreted correctly.
	rows, err := ie.QueryBufferedEx(
		ctx,
		b.selectOpName,
		nil, /* txn */
		getInternalExecutorOverride(b.User, b.DescriptorOverrides, sessiondatapb.BulkLowQoS),
		query,
		b.cachedArgs...,
	)
	if err != nil {
		return nil, false, err
	}
	b.SelectDuration.RecordValue(int64(timeutil.Since(start)))

	numRows := int64(len(rows))
	if numRows > 0 {
		// Move the cursor forward if SELECT returns rows.
		lastRow := rows[numRows-1]
		if len(lastRow) != len(b.PKColNames) {
			return nil, false, errors.AssertionFailedf("expected %d columns for last row, got %d", len(b.PKColNames), len(lastRow))
		}
		b.cachedArgs = b.cachedArgs[:len(b.cachedArgs)-len(b.Bounds.Start)]
		for _, d := range lastRow {
			b.cachedArgs = append(b.cachedArgs, d)
		}
		b.Bounds.Start = lastRow
	}

	return rows, numRows == b.SelectBatchSize, nil
}

type DeleteQueryParams struct {
	RelationName    string
	PKColNames      []string
	DeleteBatchSize int64
	TTLExpr         catpb.Expression
	DeleteDuration  *aggmetric.Histogram
	// User and DescriptorOverrides identify the session the DELETE statements
	// run as; see ttlSessionOverrides. The map is captured by reference and
	// must not be mutated after construction.
	User                username.SQLUsername
	DescriptorOverrides map[uint32]sessiondata.DescriptorOverride
}

// DeleteQueryBuilder is responsible for maintaining state around the DELETE
// portion of the TTL job.
type DeleteQueryBuilder interface {
	// Run will perform the DELETE operation on the given rows.
	Run(ctx context.Context, txn isql.Txn, rows []tree.Datums) (int64, error)

	// BuildQuery generates the DELETE query for the given number of rows.
	BuildQuery(numRows int) string

	// GetBatchSize returns the batch size for the DELETE operation.
	GetBatchSize() int
}

type deleteQueryBuilder struct {
	DeleteQueryParams
	deleteOpName redact.RedactableString
	// cachedQuery is the cached query, which stays the same as long as we are
	// deleting up to DeleteBatchSize elements.
	cachedQuery string
	// cachedArgs keeps a cache of args to use in the run query.
	// The cache is of form [cutoff, flattened PKs...].
	cachedArgs []interface{}
}

func MakeDeleteQueryBuilder(
	params DeleteQueryParams, cutoff time.Time,
) (DeleteQueryBuilder, error) {
	if len(params.PKColNames) == 0 {
		return nil, errors.AssertionFailedf("PKColNames is empty")
	}
	// An unset User would silently fall back to the internal executor's
	// default identity (the node user, which bypasses privilege checks).
	if params.User.Undefined() {
		return nil, errors.AssertionFailedf("User is unset")
	}
	cachedArgs := make([]interface{}, 0, 1+int64(len(params.PKColNames))*params.DeleteBatchSize)
	cachedArgs = append(cachedArgs, cutoff)

	return &deleteQueryBuilder{
		DeleteQueryParams: params,
		deleteOpName:      redact.Sprintf("ttl delete %s", params.RelationName),
		cachedArgs:        cachedArgs,
	}, nil
}

func (b *deleteQueryBuilder) BuildQuery(numRows int) string {
	return ttlbase.BuildDeleteQuery(
		b.RelationName,
		b.PKColNames,
		b.TTLExpr,
		numRows,
	)
}

// GetBatchSize implements the DeleteQueryBuilder interface.
func (b *deleteQueryBuilder) GetBatchSize() int {
	return int(b.DeleteBatchSize)
}

// Run implements the DeleteQueryBuilder interface.
func (b *deleteQueryBuilder) Run(
	ctx context.Context, txn isql.Txn, rows []tree.Datums,
) (int64, error) {
	numRows := len(rows)
	var query string
	if int64(numRows) == b.DeleteBatchSize {
		if b.cachedQuery == "" {
			b.cachedQuery = b.BuildQuery(numRows)
		}
		query = b.cachedQuery
	} else {
		query = b.BuildQuery(numRows)
	}

	deleteArgs := b.cachedArgs[:1]
	for _, row := range rows {
		for _, col := range row {
			// Convert any DEnum args to their logical representation to avoid the risk
			// of using the wrong version of the enum type descriptor.
			if enum, ok := col.(*tree.DEnum); ok {
				deleteArgs = append(deleteArgs, enum.LogicalRep)
			} else {
				deleteArgs = append(deleteArgs, col)
			}
		}
	}

	start := timeutil.Now()
	rowCount, err := txn.ExecEx(
		ctx,
		b.deleteOpName,
		txn.KV(),
		getInternalExecutorOverride(b.User, b.DescriptorOverrides, sessiondatapb.BulkLowQoS),
		query,
		deleteArgs...,
	)
	if err != nil {
		return 0, err
	}
	b.DeleteDuration.RecordValue(int64(timeutil.Since(start)))
	return int64(rowCount), nil
}
