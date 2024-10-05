// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// crdbInternalRangesViewDEPRECATED is the pre-v23.1
// version of `crdb_internal.ranges`. See the package-level doc
// of package `deprecatedshowranges` for details.
//
// Remove this code in v23.2.
var crdbInternalRangesViewDEPRECATED = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.ranges AS SELECT
	range_id,
	start_key,
	start_pretty,
	end_key,
	end_pretty,
  table_id,
	database_name,
  schema_name,
	table_name,
	index_name,
	replicas,
	replica_localities,
	voting_replicas,
	non_voting_replicas,
	learner_replicas,
	split_enforced_until,
	crdb_internal.lease_holder(start_key) AS lease_holder,
	(crdb_internal.range_stats(start_key)->>'key_bytes')::INT +
	(crdb_internal.range_stats(start_key)->>'val_bytes')::INT +
	coalesce((crdb_internal.range_stats(start_key)->>'range_key_bytes')::INT, 0) +
	coalesce((crdb_internal.range_stats(start_key)->>'range_val_bytes')::INT, 0) AS range_size
FROM crdb_internal.ranges_no_leases
`,
	resultColumns: colinfo.ResultColumns{
		{Name: "range_id", Typ: types.Int},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_key", Typ: types.Bytes},
		{Name: "end_pretty", Typ: types.String},
		{Name: "table_id", Typ: types.Int},
		{Name: "database_name", Typ: types.String},
		{Name: "schema_name", Typ: types.String},
		{Name: "table_name", Typ: types.String},
		{Name: "index_name", Typ: types.String},
		{Name: "replicas", Typ: types.Int2Vector},
		{Name: "replica_localities", Typ: types.StringArray},
		{Name: "voting_replicas", Typ: types.Int2Vector},
		{Name: "non_voting_replicas", Typ: types.Int2Vector},
		{Name: "learner_replicas", Typ: types.Int2Vector},
		{Name: "split_enforced_until", Typ: types.Timestamp},
		{Name: "lease_holder", Typ: types.Int},
		{Name: "range_size", Typ: types.Int},
	},
}

// crdbInternalRangesNoLeasesTableDEPRECATED is the pre-v23.1 version
// of `crdb_internal.ranges_no_leases`. See the package-level doc of
// package `deprecatedshowranges` for details.
//
// Remove this code in v23.2.
var crdbInternalRangesNoLeasesTableDEPRECATED = virtualSchemaTable{
	comment: `range metadata without leaseholder details (KV join; expensive!)`,
	// NB 1: The `replicas` column is the union of `voting_replicas` and
	// `non_voting_replicas` and does not include `learner_replicas`.
	// NB 2: All the values in the `*replicas` columns correspond to store IDs.
	schema: `
CREATE TABLE crdb_internal.ranges_no_leases (
  range_id             INT NOT NULL,
  start_key            BYTES NOT NULL,
  start_pretty         STRING NOT NULL,
  end_key              BYTES NOT NULL,
  end_pretty           STRING NOT NULL,
  table_id             INT NOT NULL,
  database_name        STRING NOT NULL,
  schema_name          STRING NOT NULL,
  table_name           STRING NOT NULL,
  index_name           STRING NOT NULL,
  replicas             INT[] NOT NULL,
  replica_localities   STRING[] NOT NULL,
  voting_replicas      INT[] NOT NULL,
  non_voting_replicas  INT[] NOT NULL,
  learner_replicas     INT[] NOT NULL,
  split_enforced_until TIMESTAMP
)
`,
	generator: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, _ *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		hasAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return nil, nil, err
		}
		all, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return nil, nil, err
		}
		descs := all.OrderedDescriptors()

		privCheckerFunc := func(desc catalog.Descriptor) (bool, error) {
			if hasAdmin {
				return true, nil
			}

			return p.HasPrivilege(ctx, desc, privilege.ZONECONFIG, p.User())
		}

		hasPermission, dbNames, tableNames, schemaNames, indexNames, schemaParents, parents, err :=
			descriptorsByType(descs, privCheckerFunc)
		if err != nil {
			return nil, nil, err
		}

		// if the user has no ZONECONFIG privilege on any table/schema/database
		if !hasPermission {
			return nil, nil, pgerror.Newf(pgcode.InsufficientPrivilege, "only users with the ZONECONFIG privilege or the admin role can read crdb_internal.ranges_no_leases")
		}

		execCfg := p.ExecCfg()
		rangeDescIterator, err := execCfg.RangeDescIteratorFactory.NewIterator(ctx, execCfg.Codec.TenantSpan())
		if err != nil {
			return nil, nil, err
		}

		return func() (tree.Datums, error) {
			if !rangeDescIterator.Valid() {
				return nil, nil
			}

			rangeDesc := rangeDescIterator.CurRangeDescriptor()

			rangeDescIterator.Next()

			replicas := rangeDesc.Replicas()
			votersAndNonVoters := append([]roachpb.ReplicaDescriptor(nil),
				replicas.VoterAndNonVoterDescriptors()...)
			var learnerReplicaStoreIDs []int
			for _, rd := range replicas.LearnerDescriptors() {
				learnerReplicaStoreIDs = append(learnerReplicaStoreIDs, int(rd.StoreID))
			}
			sort.Slice(votersAndNonVoters, func(i, j int) bool {
				return votersAndNonVoters[i].StoreID < votersAndNonVoters[j].StoreID
			})
			sort.Ints(learnerReplicaStoreIDs)
			votersAndNonVotersArr := tree.NewDArray(types.Int)
			for _, replica := range votersAndNonVoters {
				if err := votersAndNonVotersArr.Append(tree.NewDInt(tree.DInt(replica.StoreID))); err != nil {
					return nil, err
				}
			}
			votersArr := tree.NewDArray(types.Int)
			for _, replica := range replicas.VoterDescriptors() {
				if err := votersArr.Append(tree.NewDInt(tree.DInt(replica.StoreID))); err != nil {
					return nil, err
				}
			}
			nonVotersArr := tree.NewDArray(types.Int)
			for _, replica := range replicas.NonVoterDescriptors() {
				if err := nonVotersArr.Append(tree.NewDInt(tree.DInt(replica.StoreID))); err != nil {
					return nil, err
				}
			}
			learnersArr := tree.NewDArray(types.Int)
			for _, replica := range learnerReplicaStoreIDs {
				if err := learnersArr.Append(tree.NewDInt(tree.DInt(replica))); err != nil {
					return nil, err
				}
			}

			replicaLocalityArr := tree.NewDArray(types.String)
			for _, replica := range votersAndNonVoters {
				// The table should still be rendered even if node locality is unavailable,
				// so use NULL if nodeDesc is not found.
				// See https://github.com/cockroachdb/cockroach/issues/92915.
				replicaLocalityDatum := tree.DNull
				nodeDesc, err := p.ExecCfg().NodeDescs.GetNodeDescriptor(replica.NodeID)
				if err != nil {
					if !errors.HasType(err, &kvpb.DescNotFoundError{}) {
						return nil, err
					}
				} else {
					replicaLocalityDatum = tree.NewDString(nodeDesc.Locality.String())
				}
				if err := replicaLocalityArr.Append(replicaLocalityDatum); err != nil {
					return nil, err
				}
			}

			tableID, dbName, schemaName, tableName, indexName := lookupNamesByKey(
				p, rangeDesc.StartKey.AsRawKey(), dbNames, tableNames, schemaNames,
				indexNames, schemaParents, parents,
			)

			splitEnforcedUntil := tree.DNull
			if !rangeDesc.StickyBit.IsEmpty() {
				splitEnforcedUntil = eval.TimestampToInexactDTimestamp(rangeDesc.StickyBit)
			}

			return tree.Datums{
				tree.NewDInt(tree.DInt(rangeDesc.RangeID)),
				tree.NewDBytes(tree.DBytes(rangeDesc.StartKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, rangeDesc.StartKey.AsRawKey())),
				tree.NewDBytes(tree.DBytes(rangeDesc.EndKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, rangeDesc.EndKey.AsRawKey())),
				tree.NewDInt(tree.DInt(tableID)),
				tree.NewDString(dbName),
				tree.NewDString(schemaName),
				tree.NewDString(tableName),
				tree.NewDString(indexName),
				votersAndNonVotersArr,
				replicaLocalityArr,
				votersArr,
				nonVotersArr,
				learnersArr,
				splitEnforcedUntil,
			}, nil
		}, nil, nil
	},
}
