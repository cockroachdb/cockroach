// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvissubscriber

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// keyvissubscriber manages the rangefeed on the SpanStatsTenantBoundariesTable.
// When there is a rangefeed update, the update is decoded into a
// keyvispb.UpdateBoundariesRequest and is passed to the SpanStatsCollector
// via handleBoundaryUpdate.

// Start starts the rangefeed on the SpanStatsTenantBoundariesTable.
// When a new event is received, the event is decoded and delegated to handleBoundaryUpdate.
func Start(
	ctx context.Context,
	stopper *stop.Stopper,
	db *kv.DB,
	settings *cluster.Settings,
	sysTableResolver catalog.SystemTableIDResolver,
	initialTimestamp hlc.Timestamp,
	handleBoundaryUpdate func(update *keyvispb.UpdateBoundariesRequest),
) error {

	tableID, err := startup.RunIdempotentWithRetryEx(ctx, stopper.ShouldQuiesce(),
		"obs lookup system table",
		func(ctx context.Context) (descpb.ID, error) {
			return sysTableResolver.LookupSystemTableID(
				ctx, systemschema.SpanStatsTenantBoundariesTable.GetName())
		})
	if err != nil {
		return err
	}

	tablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(tableID))
	tableSpan := roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	}

	columns := systemschema.SpanStatsTenantBoundariesTable.PublicColumns()
	decoder := valueside.MakeDecoder(columns)
	da := &tree.DatumAlloc{}

	// decodeRow decodes a row of the system.span_stats_tenant_boundaries table.
	decodeRow := func(
		ctx context.Context,
		kv *kvpb.RangeFeedValue,
	) (*roachpb.TenantID, *keyvispb.UpdateBoundariesRequest, error) {

		// First, decode the tenant_id column.
		colTypes := []*types.T{columns[0].GetType()}
		tenantIDRow := make([]rowenc.EncDatum, 1)
		if _, err := rowenc.DecodeIndexKey(
			keys.SystemSQLCodec,
			tenantIDRow,
			nil, /* colDirs */
			kv.Key,
		); err != nil {
			return nil, nil, err
		}
		if err := tenantIDRow[0].EnsureDecoded(colTypes[0], da); err != nil {
			return nil, nil, err
		}

		// Next, decode the boundaries column, which is a boundary encoded proto.
		bytes, err := kv.Value.GetTuple()
		if err != nil {
			return nil, nil, err
		}

		datums, err := decoder.Decode(da, bytes)
		if err != nil {
			return nil, nil, err
		}

		decoded := keyvispb.UpdateBoundariesRequest{}
		updateEncoded := []byte(tree.MustBeDBytes(datums[1]))

		if err := protoutil.Unmarshal(updateEncoded, &decoded); err != nil {
			return nil, nil, err
		}

		tID, err := roachpb.MakeTenantID(uint64(tree.MustBeDInt(tenantIDRow[0].Datum)))
		if err != nil {
			return nil, nil, err
		}
		return &tID, &decoded, nil
	}

	f, err := rangefeed.NewFactory(stopper, db, settings, nil)
	if err != nil {
		return err
	}
	_, err = f.RangeFeed(
		ctx,
		"tenant-boundaries-watcher",
		[]roachpb.Span{tableSpan},
		initialTimestamp,
		func(ctx context.Context, kv *kvpb.RangeFeedValue) {
			tID, update, err := decodeRow(ctx, kv)
			if err != nil {
				log.Warningf(ctx,
					"tenant boundary decoding failed with error: %v", err)
				return
			}

			// A row in the system.span_stats_tenant_boundaries table might
			// belong to a secondary tenant, but we don't support that right
			// now. During a rolling update, a node that hasn't been updated
			// should just no-op if it receives a row for a secondary tenant.
			if *tID == roachpb.SystemTenantID {
				handleBoundaryUpdate(update)
			} else {
				log.Warningf(ctx,
					"can not update boundaries for secondary tenants")
			}
		},
	)
	return err
}
