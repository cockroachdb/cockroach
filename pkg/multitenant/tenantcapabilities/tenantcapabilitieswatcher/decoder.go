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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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

func (d *decoder) decode(kv roachpb.KeyValue) (tenantcapabilities.CapabilitiesEntry, error) {
	// First we decode the tenantID from the key.
	var tenID roachpb.TenantID
	types := []*types.T{d.columns[0].GetType()}
	tenantIDRow := make([]rowenc.EncDatum, 1)
	_, _, err := rowenc.DecodeIndexKey(keys.SystemSQLCodec, types, tenantIDRow, nil /* colDirs */, kv.Key)
	if err != nil {
		return tenantcapabilities.CapabilitiesEntry{}, err
	}
	if err := tenantIDRow[0].EnsureDecoded(types[0], &d.alloc); err != nil {
		return tenantcapabilities.CapabilitiesEntry{},
			errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode key in system.tenants %v", kv.Key)
	}
	tenID, err = roachpb.MakeTenantID(uint64(tree.MustBeDInt(tenantIDRow[0].Datum)))
	if err != nil {
		return tenantcapabilities.CapabilitiesEntry{}, err
	}

	// The remaining columns are stored in the value; we're just interested in the
	// info column.
	if !kv.Value.IsPresent() {
		return tenantcapabilities.CapabilitiesEntry{},
			errors.AssertionFailedf("missing value for tenant: %v", tenID)
	}

	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return tenantcapabilities.CapabilitiesEntry{}, err
	}
	datums, err := d.decoder.Decode(&d.alloc, bytes)
	if err != nil {
		return tenantcapabilities.CapabilitiesEntry{}, err
	}

	var tenantInfo descpb.TenantInfo
	if i := datums[2]; i != tree.DNull {
		infoBytes := tree.MustBeDBytes(i)
		if err := protoutil.Unmarshal([]byte(infoBytes), &tenantInfo); err != nil {
			return tenantcapabilities.CapabilitiesEntry{}, errors.Wrapf(err, "failed to unmarshall tenant info")
		}
	}

	return tenantcapabilities.CapabilitiesEntry{
		TenantID:           tenID,
		TenantCapabilities: tenantInfo.Capabilities,
	}, nil
}
