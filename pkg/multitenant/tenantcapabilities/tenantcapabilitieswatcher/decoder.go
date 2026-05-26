// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilitieswatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfo"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

// decoder decodes rows from system.tenants. It's not
// safe for concurrent use.
type decoder struct {
	alloc   tree.DatumAlloc
	columns []catalog.Column
	decoder valueside.Decoder
	st      *cluster.Settings
}

// newDecoder constructs and returns a decoder.
func newDecoder(st *cluster.Settings) *decoder {
	columns := systemschema.TenantsTable.VisibleColumns()
	return &decoder{
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
		st:      st,
	}
}

func (d *decoder) decode(
	ctx context.Context, kv roachpb.KeyValue,
) (tenantcapabilities.Entry, error) {
	// First we decode the tenantID from the key.
	types := []*types.T{d.columns[0].GetType()}
	tenantIDRow := make([]rowenc.EncDatum, 1)
	if _, err := rowenc.DecodeIndexKey(keys.SystemSQLCodec, tenantIDRow, nil /* colDirs */, kv.Key); err != nil {
		return tenantcapabilities.Entry{}, err
	}
	if err := tenantIDRow[0].EnsureDecoded(types[0], &d.alloc); err != nil {
		return tenantcapabilities.Entry{},
			errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode key in system.tenants %v", kv.Key)
	}

	if !kv.Value.IsPresent() {
		return tenantcapabilities.Entry{},
			errors.AssertionFailedf("missing value for tenant: %v", tenantIDRow[0].Datum)
	}

	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return tenantcapabilities.Entry{}, err
	}
	datums, err := d.decoder.Decode(&d.alloc, bytes)
	if err != nil {
		return tenantcapabilities.Entry{}, err
	}

	// The tenant ID is the first column and comes from the PK decoder above.
	datums[0] = tenantIDRow[0].Datum

	tid, info, err := mtinfo.GetTenantInfoFromSQLRow(datums)
	if err != nil {
		return tenantcapabilities.Entry{}, err
	}

	return tenantcapabilities.Entry{
		TenantID:           tid,
		TenantCapabilities: &info.Capabilities,
		Name:               info.Name,
		DataState:          info.DataState,
		ServiceMode:        info.ServiceMode,
	}, nil
}

func (d *decoder) translateEvent(
	ctx context.Context, ev *kvpb.RangeFeedValue,
) (hasEvent bool, event tenantcapabilities.Update) {
	deleted := !ev.Value.IsPresent()
	var value roachpb.Value
	// The event corresponds to a deletion. The capabilities being deleted must
	// be read from PrevValue.
	if deleted {
		// There's nothing for us to do if this event corresponds to a deletion
		// tombstone being removed (GC).
		if !ev.PrevValue.IsPresent() {
			return false, event
		}

		value = ev.PrevValue
	} else {
		// Not a deletion event; read capabilities off Value.
		value = ev.Value
	}

	entry, err := d.decode(ctx, roachpb.KeyValue{
		Key:   ev.Key,
		Value: value,
	})
	if err != nil {
		// This should never happen: the rangefeed should only ever deliver valid SQL rows.
		err = errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode row %v", ev.Key)
		logcrash.ReportOrPanic(ctx, &d.st.SV, "%w", err)
		return false, event
	}
	return true, tenantcapabilities.Update{
		Entry:     entry,
		Deleted:   deleted,
		Timestamp: ev.Value.Timestamp,
	}
}

// TestingDecoderFn exports the decoding routine for testing purposes.
func TestingDecoderFn(
	st *cluster.Settings,
) func(context.Context, roachpb.KeyValue) (tenantcapabilities.Entry, error) {
	return newDecoder(st).decode
}
