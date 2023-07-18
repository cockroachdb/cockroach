// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ResultColumn contains the name and type of a SQL "cell".
type ResultColumn struct {
	Name string
	Typ  *types.T

	// If set, this is an implicit column; used internally.
	Hidden bool

	// TableID/PGAttributeNum identify the source of the column, if it is a simple
	// reference to a column of a base table (or view). If it is not a simple
	// reference, these fields are zeroes.
	TableID        descpb.ID // OID of column's source table (pg_attribute.attrelid).
	PGAttributeNum uint32    // Column's number in source table (pg_attribute.attnum).
}

// ResultColumns is the type used throughout the sql module to
// describe the column types of a table.
type ResultColumns []ResultColumn

// ResultColumnsFromColumns converts []catalog.Column to []ResultColumn.
func ResultColumnsFromColumns(tableID descpb.ID, columns []catalog.Column) ResultColumns {
	return ResultColumnsFromColDescs(tableID, len(columns), func(i int) *descpb.ColumnDescriptor {
		return columns[i].ColumnDesc()
	})
}

// ResultColumnsFromColDescs is used by ResultColumnsFromColumns and by tests.
func ResultColumnsFromColDescs(
	tableID descpb.ID, numCols int, getColDesc func(int) *descpb.ColumnDescriptor,
) ResultColumns {
	cols := make(ResultColumns, numCols)
	for i := range cols {
		colDesc := getColDesc(i)
		typ := colDesc.Type
		if typ == nil {
			panic(errors.AssertionFailedf("unsupported column type: %s", colDesc.Type.Family()))
		}
		cols[i] = ResultColumn{
			Name:           colDesc.Name,
			Typ:            typ,
			Hidden:         colDesc.Hidden,
			TableID:        tableID,
			PGAttributeNum: uint32(colDesc.GetPGAttributeNum()),
		}
	}
	return cols
}

// GetTypeModifier returns the type modifier for this column. If it is not set,
// it defaults to returning -1.
func (r ResultColumn) GetTypeModifier() int32 {
	return r.Typ.TypeModifier()
}

// TypesEqual returns whether the length and types of r matches other. If
// a type in other is NULL, it is considered equal.
func (r ResultColumns) TypesEqual(other ResultColumns) bool {
	if len(r) != len(other) {
		return false
	}
	for i, c := range r {
		// NULLs are considered equal because some types of queries (SELECT CASE,
		// for example) can change their output types between a type and NULL based
		// on input.
		if other[i].Typ.Family() == types.UnknownFamily {
			continue
		}
		if !c.Typ.Equivalent(other[i].Typ) {
			return false
		}
	}
	return true
}

// NodeFormatter returns a tree.NodeFormatter that, when formatted,
// represents the column at the input column index.
func (r ResultColumns) NodeFormatter(colIdx int) tree.NodeFormatter {
	return &varFormatter{ColumnName: tree.Name(r[colIdx].Name)}
}

// String formats result columns to a string.
// The column types are printed if printTypes is true.
// The hidden property is printed if showHidden is true.
func (r ResultColumns) String(printTypes bool, showHidden bool) string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteByte('(')
	for i := range r {
		rCol := &r[i]
		if i > 0 {
			f.WriteString(", ")
		}
		f.FormatNameP(&rCol.Name)
		// Output extra properties like [hidden,omitted].
		hasProps := false
		outputProp := func(prop string) {
			if hasProps {
				f.WriteByte(',')
			} else {
				f.WriteByte('[')
			}
			hasProps = true
			f.WriteString(prop)
		}
		if showHidden && rCol.Hidden {
			outputProp("hidden")
		}
		if hasProps {
			f.WriteByte(']')
		}

		if printTypes {
			f.WriteByte(' ')
			f.WriteString(rCol.Typ.String())
		}
	}
	f.WriteByte(')')
	return f.CloseAndGetString()
}

// ExplainPlanColumns are the result columns of various EXPLAIN variants.
var ExplainPlanColumns = ResultColumns{
	{Name: "info", Typ: types.String},
}

// ShowCommitTimestampColumns are the result columns of SHOW COMMIT TIMESTAMP.
var ShowCommitTimestampColumns = ResultColumns{
	{Name: "commit_timestamp", Typ: types.Decimal},
}

// ShowTraceColumns are the result columns of a SHOW [KV] TRACE statement.
var ShowTraceColumns = ResultColumns{
	{Name: "timestamp", Typ: types.TimestampTZ},
	{Name: "age", Typ: types.Interval}, // Note GetTraceAgeColumnIdx below.
	{Name: "message", Typ: types.String},
	{Name: "tag", Typ: types.String},
	{Name: "location", Typ: types.String},
	{Name: "operation", Typ: types.String},
	{Name: "span", Typ: types.Int},
}

// ShowCompactTraceColumns are the result columns of a
// SHOW COMPACT [KV] TRACE statement.
var ShowCompactTraceColumns = ResultColumns{
	{Name: "age", Typ: types.Interval}, // Note GetTraceAgeColumnIdx below.
	{Name: "message", Typ: types.String},
	{Name: "tag", Typ: types.String},
	{Name: "operation", Typ: types.String},
}

// GetTraceAgeColumnIdx retrieves the index of the age column
// depending on whether the compact format is used.
func GetTraceAgeColumnIdx(compact bool) int {
	if compact {
		return 0
	}
	return 1
}

// ShowReplicaTraceColumns are the result columns of a
// SHOW EXPERIMENTAL_REPLICA TRACE statement.
var ShowReplicaTraceColumns = ResultColumns{
	{Name: "timestamp", Typ: types.TimestampTZ},
	{Name: "node_id", Typ: types.Int},
	{Name: "store_id", Typ: types.Int},
	{Name: "replica_id", Typ: types.Int},
}

// ShowSyntaxColumns are the columns of a SHOW SYNTAX statement.
var ShowSyntaxColumns = ResultColumns{
	{Name: "field", Typ: types.String},
	{Name: "message", Typ: types.String},
}

// ShowFingerprintsColumns are the result columns of a
// SHOW EXPERIMENTAL_FINGERPRINTS statement.
var ShowFingerprintsColumns = ResultColumns{
	{Name: "index_name", Typ: types.String},
	{Name: "fingerprint", Typ: types.String},
}

// ShowCompletionsColumns are the result columns of a
// SHOW COMPLETIONS statement.
var ShowCompletionsColumns = ResultColumns{
	{Name: "completion", Typ: types.String},
	{Name: "category", Typ: types.String},
	{Name: "description", Typ: types.String},
	{Name: "start", Typ: types.Int},
	{Name: "end", Typ: types.Int},
}

// AlterTableSplitColumns are the result columns of an
// ALTER TABLE/INDEX .. SPLIT AT statement.
var AlterTableSplitColumns = ResultColumns{
	{Name: "key", Typ: types.Bytes},
	{Name: "pretty", Typ: types.String},
	{Name: "split_enforced_until", Typ: types.Timestamp},
}

// AlterTableUnsplitColumns are the result columns of an
// ALTER TABLE/INDEX .. UNSPLIT statement.
var AlterTableUnsplitColumns = ResultColumns{
	{Name: "key", Typ: types.Bytes},
	{Name: "pretty", Typ: types.String},
}

// AlterTableRelocateColumns are the result columns of an
// ALTER TABLE/INDEX .. EXPERIMENTAL_RELOCATE statement.
var AlterTableRelocateColumns = ResultColumns{
	{Name: "key", Typ: types.Bytes},
	{Name: "pretty", Typ: types.String},
}

// AlterTableScatterColumns are the result columns of an
// ALTER TABLE/INDEX .. SCATTER statement.
var AlterTableScatterColumns = ResultColumns{
	{Name: "key", Typ: types.Bytes},
	{Name: "pretty", Typ: types.String},
}

// AlterRangeRelocateColumns are the result columns of an
// ALTER RANGE .. RELOCATE statement.
var AlterRangeRelocateColumns = ResultColumns{
	{Name: "range_id", Typ: types.Int},
	{Name: "pretty", Typ: types.String},
	{Name: "result", Typ: types.String},
}

// ScrubColumns are the result columns of a SCRUB statement.
var ScrubColumns = ResultColumns{
	{Name: "job_uuid", Typ: types.Uuid},
	{Name: "error_type", Typ: types.String},
	{Name: "database", Typ: types.String},
	{Name: "table", Typ: types.String},
	{Name: "primary_key", Typ: types.String},
	{Name: "timestamp", Typ: types.Timestamp},
	{Name: "repaired", Typ: types.Bool},
	{Name: "details", Typ: types.Jsonb},
}

// SequenceSelectColumns are the result columns of a sequence data source.
var SequenceSelectColumns = ResultColumns{
	{Name: `last_value`, Typ: types.Int},
	{Name: `log_cnt`, Typ: types.Int},
	{Name: `is_called`, Typ: types.Bool},
}

// ExportColumns are the result columns of an EXPORT statement (i.e. a user will
// see a table with these columns in their sql shell after EXPORT returns).
// These columns differ from the logical columns in the export file.
var ExportColumns = ResultColumns{
	{Name: "filename", Typ: types.String},
	{Name: "rows", Typ: types.Int},
	{Name: "bytes", Typ: types.Int},
}

// ExportColumnTypes is the type schema of the EXPORT statement.
var ExportColumnTypes []*types.T

func init() {
	ExportColumnTypes = make([]*types.T, len(ExportColumns))
	for i, c := range ExportColumns {
		ExportColumnTypes[i] = c.Typ
	}
}

// TenantColumns appear in all SHOW VIRTUAL CLUSTER queries.
var TenantColumns = ResultColumns{
	{Name: "id", Typ: types.Int},
	{Name: "name", Typ: types.String},
	{Name: "data_state", Typ: types.String},
	{Name: "service_mode", Typ: types.String},
}

// TenantColumnsWithReplication is appended to TenantColumns for
// SHOW VIRTUAL CLUSTER ... WITH REPLICATION STATUS queries.
var TenantColumnsWithReplication = ResultColumns{
	{Name: "source_tenant_name", Typ: types.String},
	{Name: "source_cluster_uri", Typ: types.String},
	{Name: "replication_job_id", Typ: types.Int},
	// The latest fully replicated time.
	{Name: "replicated_time", Typ: types.TimestampTZ},
	// The protected timestamp on the destination cluster, meaning we cannot
	// cutover to before this time.
	{Name: "retained_time", Typ: types.TimestampTZ},
	{Name: "cutover_time", Typ: types.Decimal},
}

// TenantColumnsWithCapabilities is appended to TenantColumns for
// SHOW VIRTUAL CLUSTER ... WITH CAPABILITIES queries.
var TenantColumnsWithCapabilities = ResultColumns{
	{Name: "capability_name", Typ: types.String},
	{Name: "capability_value", Typ: types.String},
}

// RangesNoLeases is the schema for crdb_internal.ranges_no_leases.
var RangesNoLeases = ResultColumns{
	{Name: "range_id", Typ: types.Int},
	{Name: "start_key", Typ: types.Bytes},
	{Name: "start_pretty", Typ: types.String},
	{Name: "end_key", Typ: types.Bytes},
	{Name: "end_pretty", Typ: types.String},
	{Name: "replicas", Typ: types.IntArray},
	{Name: "replica_localities", Typ: types.StringArray},
	{Name: "voting_replicas", Typ: types.IntArray},
	{Name: "non_voting_replicas", Typ: types.IntArray},
	{Name: "learner_replicas", Typ: types.IntArray},
	{Name: "split_enforced_until", Typ: types.Timestamp},
}

// Ranges is the schema for crdb_internal.ranges.
var Ranges = append(
	RangesNoLeases,
	// The following columns are computed by RangesExtraRenders below.
	ResultColumn{Name: "lease_holder", Typ: types.Int},
	ResultColumn{Name: "range_size", Typ: types.Int},
)

// RangesExtraRenders describes the extra projections in
// crdb_internal.ranges not included in crdb_internal.ranges_no_leases.
const RangesExtraRenders = `
	crdb_internal.lease_holder(start_key) AS lease_holder,
	(crdb_internal.range_stats(start_key)->>'key_bytes')::INT +
	(crdb_internal.range_stats(start_key)->>'val_bytes')::INT +
	coalesce((crdb_internal.range_stats(start_key)->>'range_key_bytes')::INT, 0) +
	coalesce((crdb_internal.range_stats(start_key)->>'range_val_bytes')::INT, 0) AS range_size
`

// IdentifySystemColumns is the schema for IDENTIFY_SYSTEM.
var IdentifySystemColumns = ResultColumns{
	{Name: "systemid", Typ: types.String},
	{Name: "timeline", Typ: types.Int4},
	{Name: "xlogpos", Typ: types.String},
	{Name: "dbname", Typ: types.String},
}
