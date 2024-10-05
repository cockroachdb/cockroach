// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package delegate

import (
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/deprecatedshowranges"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

func checkPrivilegesForShowRangesDEPRECATED(d *delegator, table cat.Table) error {
	// Basic requirement is SELECT priviliges
	if err := d.catalog.CheckPrivilege(d.ctx, table, privilege.SELECT); err != nil {
		return err
	}
	hasAdmin, err := d.catalog.HasAdminRole(d.ctx)
	if err != nil {
		return err
	}
	// User needs to either have admin access or have the correct ZONECONFIG privilege
	if hasAdmin {
		return nil
	}
	if err := d.catalog.CheckPrivilege(d.ctx, table, privilege.ZONECONFIG); err != nil {
		return pgerror.Wrapf(err, pgcode.InsufficientPrivilege, "only users with the ZONECONFIG privilege or the admin role can use SHOW RANGES on %s", table.Name())
	}
	return nil
}

// delegateShowRangesDEPRECATED implements the SHOW RANGES statement
// using the semantics prior to the introduction of range coalescing.
//
//	SHOW RANGES FROM TABLE t
//	SHOW RANGES FROM INDEX t@idx
//	SHOW RANGES FROM DATABASE db
//
// These statements show the ranges corresponding to the given table or index,
// along with the list of replicas and the lease holder.
func (d *delegator) delegateShowRangesDEPRECATED(n *tree.ShowRanges) (tree.Statement, error) {
	switch n.Source {
	case tree.ShowRangesDatabase, tree.ShowRangesTable, tree.ShowRangesIndex:
		// These are supported by this pre-v23.1 implementation.
	default:
		err := pgerror.New(pgcode.Syntax,
			"the deprecated syntax of SHOW RANGES only supports FROM TABLE, FROM INDEX or FROM DATABASE")
		err = errors.WithDetail(err,
			"The behavior of SHOW RANGES is currently restricted by configuration to its pre-v23.1 behavior.")
		err = errors.WithHint(err,
			"To access the new syntax and semantics, toggle the cluster setting "+deprecatedshowranges.ShowRangesDeprecatedBehaviorSettingName+".")
		return nil, err
	}

	sqltelemetry.IncrementShowCounter(sqltelemetry.Ranges)
	if n.DatabaseName != "" {
		const dbQuery = `
		SELECT
			table_name,
			CASE
				WHEN crdb_internal.pretty_key(r.start_key, 2) = '' THEN NULL
				ELSE crdb_internal.pretty_key(r.start_key, 2)
			END AS start_key,
			CASE
				WHEN crdb_internal.pretty_key(r.end_key, 2) = '' THEN NULL
				ELSE crdb_internal.pretty_key(r.end_key, 2)
			END AS end_key,
			range_id,
			range_size / 1000000 as range_size_mb,
			lease_holder,
			replica_localities[array_position(replicas, lease_holder)] as lease_holder_locality,
			replicas,
			replica_localities,
			voting_replicas,
			non_voting_replicas
		FROM %[1]s.crdb_internal.ranges AS r
		WHERE database_name=%[2]s
		ORDER BY table_name, r.start_key
		`
		// Note: n.DatabaseName.String() != string(n.DatabaseName)
		return d.parse(fmt.Sprintf(dbQuery, n.DatabaseName.String(), lexbase.EscapeSQLString(string(n.DatabaseName))))
	}

	// Remember the original syntax: Resolve below modifies the TableOrIndex struct in-place.
	noIndexSpecified := n.TableOrIndex.Index == ""

	idx, resName, err := cat.ResolveTableIndex(
		d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, &n.TableOrIndex,
	)
	if err != nil {
		return nil, err
	}

	if err := checkPrivilegesForShowRangesDEPRECATED(d, idx.Table()); err != nil {
		return nil, err
	}

	if idx.Table().IsVirtualTable() {
		return nil, errors.New("SHOW RANGES may not be called on a virtual table")
	}

	var startKey, endKey string
	if noIndexSpecified {
		// All indexes.
		tableID := idx.Table().ID()
		prefix := d.evalCtx.Codec.TablePrefix(uint32(tableID))
		startKey = hex.EncodeToString(prefix)
		endKey = hex.EncodeToString(prefix.PrefixEnd())
	} else {
		// Just one index.
		span := idx.Span()
		startKey = hex.EncodeToString(span.Key)
		endKey = hex.EncodeToString(span.EndKey)
	}

	return d.parse(fmt.Sprintf(`
SELECT 
  CASE WHEN r.start_key <= x'%[1]s' THEN NULL ELSE crdb_internal.pretty_key(r.start_key, 2) END AS start_key,
  CASE WHEN r.end_key >= x'%[2]s' THEN NULL ELSE crdb_internal.pretty_key(r.end_key, 2) END AS end_key,
  index_name,
  range_id,
  range_size / 1000000 as range_size_mb,
  lease_holder,
  replica_localities[array_position(replicas, lease_holder)] as lease_holder_locality,
  replicas,
  replica_localities,
  voting_replicas,
  non_voting_replicas
FROM %[3]s.crdb_internal.ranges AS r
WHERE (r.start_key < x'%[2]s')
  AND (r.end_key   > x'%[1]s') ORDER BY index_name, r.start_key
`,
		startKey, endKey, resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
	))
}
