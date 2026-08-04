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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/semenumpb"
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

// fkOp identifies the kind of write a table in the foreign-key cascade closure
// receives from the TTL delete: rows deleted, or rows updated (SET NULL / SET
// DEFAULT rewrite the referencing columns). The kind of write determines which
// privileges the statement needs on the table and which of the table's own
// referential actions fire next (ON DELETE vs ON UPDATE).
type fkOp uint8

const (
	fkDelete fkOp = 1 << iota
	fkUpdate
)

// ttlSessionOverrides returns the identity that TTL statements against the given
// table run as — the table's owner — together with the per-descriptor overrides
// that identity needs.
//
// The row-level TTL job issues its SELECT, DELETE, and row-count statements
// through the internal executor. Running them as the table's owner rather than
// the internal node user keeps any user-defined code the statements reach — the
// table's row-level DELETE triggers, and the CHECK/computed-column functions
// re-evaluated by foreign-key cascades — executing with the owner's ordinary
// privileges. This matches how other background jobs that touch user data run as
// the relevant object's owner.
//
// Running as the owner is not, by itself, sufficient: deleting from the TTL
// table also touches every table reachable by a foreign-key referential action,
// and CockroachDB requires the executing user to hold privileges on those tables
// (the node user satisfied this implicitly). The returned DescriptorOverride map
// grants the owner the access those actions require:
//
//   - On each table in the cascade closure, SELECT plus DELETE or UPDATE
//     according to the kind of write cascades perform on it; referencing tables
//     that are only existence-checked (RESTRICT / NO ACTION) get SELECT alone.
//     Every grant carries an RLS bypass so that row-level security policies
//     cannot hide expired rows from the job, matching PostgreSQL's rule that
//     referential integrity is exempt from RLS.
//   - EXECUTE on the functions each of those tables references — trigger
//     routines, and the CHECK/computed-column/index expressions that are built
//     whenever the table is scanned.
//   - For tables that cascades UPDATE: SELECT on the referenced tables of their
//     outbound foreign keys (rewriting the referencing columns re-checks those
//     constraints with an existence scan), and USAGE on the sequences their
//     columns use (ON DELETE SET DEFAULT can evaluate a nextval() default).
//   - USAGE/CONNECT on the containing schemas and databases for name
//     resolution.
//
// A DescriptorOverride grants only per-descriptor privileges, never a global
// privilege, so the owner gains no authority beyond what the referential actions
// need to run.
//
// lookupTable resolves a table descriptor by ID and is used to walk the
// transitive foreign-key closure.
//
// runAsOwner is the value of the sql.ttl.run_as_table_owner.unsafe.enabled cluster
// setting. When it is false, the job reverts to the previous behavior of running
// as the internal node user with no overrides; this is an emergency opt-out (see
// runAsTableOwnerEnabled). The node user is returned explicitly rather than left
// unset so the query builders' "user must be set" invariant still holds.
func ttlSessionOverrides(
	ctx context.Context, lookupTable tableLookupFn, root catalog.TableDescriptor, runAsOwner bool,
) (username.SQLUsername, map[uint32]sessiondata.DescriptorOverride, error) {
	if !runAsOwner {
		return username.NodeUserName(), nil, nil
	}

	// Fast path: a table with no inbound foreign keys and no referenced functions
	// reaches no user-defined code (no cascades re-evaluate a child's
	// constraints or triggers, and the table itself has no DELETE triggers or
	// CHECK/computed-column UDFs). There is nothing to run with the owner's
	// privileges, so keep the previous node-user path unchanged for the common
	// case. GetAllReferencedFunctionIDs covers trigger, CHECK-constraint,
	// computed-column, index, and policy routines.
	rootFnIDs, err := root.GetAllReferencedFunctionIDs()
	if err != nil {
		return username.SQLUsername{}, nil, errors.Wrapf(err,
			"collecting functions referenced by TTL table %q", root.GetName())
	}
	if len(root.InboundForeignKeys()) == 0 && rootFnIDs.Empty() {
		return username.NodeUserName(), nil, nil
	}

	owner := root.GetPrivileges().Owner()
	if owner.Undefined() {
		// Every table descriptor has an owner (backfilled on read for pre-20.2
		// descriptors). An undefined owner here would make the internal executor
		// silently fall back to its default node identity.
		return username.SQLUsername{}, nil, errors.AssertionFailedf(
			"TTL table %q (%d) has no owner", root.GetName(), root.GetID())
	}

	overrides := map[uint32]sessiondata.DescriptorOverride{}

	// addPrivs ORs privileges into the override entry for id. Entries only ever
	// gain privileges, so reaching a descriptor again via another foreign-key
	// path is harmless regardless of order.
	addPrivs := func(id descpb.ID, privs uint64, bypassRLS bool) {
		o := overrides[uint32(id)]
		o.Privileges |= privs
		o.BypassRLS = o.BypassRLS || bypassRLS
		overrides[uint32(id)] = o
	}

	selectPriv := privilege.List{privilege.SELECT}.ToBitField()
	deletePriv := privilege.List{privilege.DELETE}.ToBitField()
	updatePriv := privilege.List{privilege.UPDATE}.ToBitField()
	execPriv := privilege.List{privilege.EXECUTE}.ToBitField()
	usagePriv := privilege.List{privilege.USAGE}.ToBitField()
	connectPriv := privilege.List{privilege.CONNECT}.ToBitField()

	// grant gives the owner privs on the table itself plus the surrounding
	// access any statement touching the table needs: EXECUTE on the functions
	// the table references, and USAGE/CONNECT on the containing schema and
	// database.
	grant := func(d catalog.TableDescriptor, tablePrivs uint64) error {
		addPrivs(d.GetID(), tablePrivs, true /* bypassRLS */)
		addPrivs(d.GetParentID(), connectPriv, false /* bypassRLS */)
		addPrivs(d.GetParentSchemaID(), usagePriv, false /* bypassRLS */)
		fnIDs, err := d.GetAllReferencedFunctionIDs()
		if err != nil {
			return errors.Wrapf(err,
				"collecting functions referenced by table %q (%d) in the foreign-key closure of TTL table %q",
				d.GetName(), d.GetID(), root.GetName())
		}
		for _, id := range fnIDs.Ordered() {
			addPrivs(id, execPriv, false /* bypassRLS */)
		}
		return nil
	}

	lookup := func(id descpb.ID) (catalog.TableDescriptor, error) {
		d, err := lookupTable(ctx, id)
		return d, errors.Wrapf(err,
			"resolving table %d in the foreign-key closure of TTL table %q", id, root.GetName())
	}

	// Breadth-first walk of the foreign-key cascade closure over (table,
	// operation) states: the TTL table receives a DELETE, an ON DELETE CASCADE
	// child of a deleted table receives a DELETE, and a SET NULL / SET DEFAULT
	// child — or any child of an updated table whose ON UPDATE action writes it
	// — receives an UPDATE. A table can receive both operations via different
	// paths (its grants merge), and cycles terminate because each (table,
	// operation) state is visited at most once. Referencing tables whose action
	// is RESTRICT / NO ACTION are only existence-checked: they get a SELECT
	// grant but fire no further cascades, unless a writing edge reaches them by
	// another path.
	type workItem struct {
		desc catalog.TableDescriptor
		op   fkOp
	}
	visited := map[descpb.ID]fkOp{root.GetID(): fkDelete}
	// scanGranted tracks tables that already received the SELECT-only grant, to
	// avoid redundant descriptor lookups. It deliberately does not stop a
	// writing edge from enqueueing the same table later.
	scanGranted := map[descpb.ID]bool{}
	queue := []workItem{{desc: root, op: fkDelete}}
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		d := item.desc

		tablePrivs := selectPriv | deletePriv
		if item.op == fkUpdate {
			tablePrivs = selectPriv | updatePriv
		}
		if err := grant(d, tablePrivs); err != nil {
			return username.SQLUsername{}, nil, err
		}

		if item.op == fkUpdate {
			// A cascaded UPDATE re-checks the table's outbound foreign keys whose
			// columns were written, which scans the referenced tables, and an ON
			// DELETE SET DEFAULT can evaluate a nextval() default. Rather than
			// computing exact column overlap, grant read access for all outbound
			// references and USAGE on all column sequences.
			for _, ofk := range d.OutboundForeignKeys() {
				refID := ofk.GetReferencedTableID()
				if visited[refID] != 0 || scanGranted[refID] {
					continue
				}
				scanGranted[refID] = true
				ref, err := lookup(refID)
				if err != nil {
					return username.SQLUsername{}, nil, err
				}
				if err := grant(ref, selectPriv); err != nil {
					return username.SQLUsername{}, nil, err
				}
			}
			for _, col := range d.PublicColumns() {
				for i := 0; i < col.NumUsesSequences(); i++ {
					addPrivs(col.GetUsesSequenceID(i), usagePriv, false /* bypassRLS */)
				}
			}
		}

		for _, fk := range d.InboundForeignKeys() {
			action := fk.OnDelete()
			if item.op == fkUpdate {
				action = fk.OnUpdate()
			}

			var childOp fkOp
			switch action {
			case semenumpb.ForeignKeyAction_CASCADE:
				// ON DELETE CASCADE deletes the child rows; ON UPDATE CASCADE
				// rewrites them.
				childOp = item.op
			case semenumpb.ForeignKeyAction_SET_NULL, semenumpb.ForeignKeyAction_SET_DEFAULT:
				childOp = fkUpdate
			default:
				// RESTRICT / NO ACTION: existence check only; no write occurs, so
				// no further cascades fire from this edge.
				childID := fk.GetOriginTableID()
				if visited[childID] != 0 || scanGranted[childID] {
					continue
				}
				scanGranted[childID] = true
				child, err := lookup(childID)
				if err != nil {
					return username.SQLUsername{}, nil, err
				}
				if err := grant(child, selectPriv); err != nil {
					return username.SQLUsername{}, nil, err
				}
				continue
			}

			childID := fk.GetOriginTableID()
			if visited[childID]&childOp != 0 {
				continue
			}
			visited[childID] |= childOp
			child, err := lookup(childID)
			if err != nil {
				return username.SQLUsername{}, nil, err
			}
			queue = append(queue, workItem{desc: child, op: childOp})
		}
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
	RelationName      string
	PKColNames        []string
	DeleteBatchSize   int64
	TTLExpr           catpb.Expression
	DeleteDuration    *aggmetric.Histogram
	DeleteRateLimiter *quotapool.RateLimiter
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

	tokens, err := b.DeleteRateLimiter.Acquire(ctx, int64(numRows))
	if err != nil {
		return 0, err
	}
	defer tokens.Consume()

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
