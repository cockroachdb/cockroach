// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitieswatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// decoder decodes rows from system.tenants. It's not
// safe for concurrent use.
type decoder struct {
	alloc   tree.DatumAlloc
	columns []catalog.Column
	decoder valueside.Decoder
}

// newDecoder constructs and returns a decoder.
func newDecoder() *decoder {
	columns := systemschema.TenantsTable.PublicColumns()
	return &decoder{
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
	}
}

func (d *decoder) decode(kv roachpb.KeyValue) (tenantcapabilities.Entry, error) {
	// First we decode the tenantID from the key.
	var tenID roachpb.TenantID
	types := []*types.T{d.columns[0].GetType()}
	tenantIDRow := make([]rowenc.EncDatum, 1)
	if _, err := rowenc.DecodeIndexKey(keys.SystemSQLCodec, tenantIDRow, nil /* colDirs */, kv.Key); err != nil {
		return tenantcapabilities.Entry{}, err
	}
	if err := tenantIDRow[0].EnsureDecoded(types[0], &d.alloc); err != nil {
		return tenantcapabilities.Entry{},
			errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode key in system.tenants %v", kv.Key)
	}
	tenID, err := roachpb.MakeTenantID(uint64(tree.MustBeDInt(tenantIDRow[0].Datum)))
	if err != nil {
		return tenantcapabilities.Entry{}, err
	}

	// The remaining columns are stored in the value; we're just interested in the
	// info column.
	if !kv.Value.IsPresent() {
		return tenantcapabilities.Entry{},
			errors.AssertionFailedf("missing value for tenant: %v", tenID)
	}

	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return tenantcapabilities.Entry{}, err
	}
	datums, err := d.decoder.Decode(&d.alloc, bytes)
	if err != nil {
		return tenantcapabilities.Entry{}, err
	}

	var tenantInfo mtinfopb.ProtoInfo
	if i := datums[2]; i != tree.DNull {
		infoBytes := tree.MustBeDBytes(i)
		if err := protoutil.Unmarshal([]byte(infoBytes), &tenantInfo); err != nil {
			return tenantcapabilities.Entry{}, errors.Wrapf(err, "failed to unmarshall tenant info")
		}
	}

	return tenantcapabilities.Entry{
		TenantID:           tenID,
		TenantCapabilities: &tenantInfo.Capabilities,
	}, nil
}

func (d *decoder) translateEvent(
	ctx context.Context, ev *kvpb.RangeFeedValue,
) rangefeedbuffer.Event {
	deleted := !ev.Value.IsPresent()
	var value roachpb.Value
	// The event corresponds to a deletion. The capabilities being deleted must
	// be read from PrevValue.
	if deleted {
		// There's nothing for us to do if this event corresponds to a deletion
		// tombstone being removed (GC).
		if !ev.PrevValue.IsPresent() {
			return nil
		}

		value = ev.PrevValue
	} else {
		// Not a deletion event; read capabilities off Value.
		value = ev.Value
	}

	entry, err := d.decode(roachpb.KeyValue{
		Key:   ev.Key,
		Value: value,
	})
	if err != nil {
		log.Fatalf(ctx, "failed to decode row: %v", err)
	}

	return &bufferEvent{
		update: tenantcapabilities.Update{
			Entry:   entry,
			Deleted: deleted,
		},
		ts: ev.Value.Timestamp,
	}
}

// TestingDecoderFn exports the decoding routine for testing purposes.
func TestingDecoderFn() func(roachpb.KeyValue) (tenantcapabilities.Entry, error) {
	return newDecoder().decode
}
