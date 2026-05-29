// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rgsubscriber

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// rowDecoder decodes a single rangefeed event from
// system.tenant_resource_groups.
type rowDecoder struct {
	alloc   tree.DatumAlloc
	columns []catalog.Column
	decoder valueside.Decoder
}

func makeRowDecoder() rowDecoder {
	cols := systemschema.TenantResourceGroupsTable.PublicColumns()
	return rowDecoder{
		columns: cols,
		decoder: valueside.MakeDecoder(cols),
	}
}

// decode parses one KV from a rangefeed event. tombstone is true for
// delete events; in that case only tenantID and id are meaningful and
// cfg is nil.
func (d *rowDecoder) decode(
	kv roachpb.KeyValue,
) (
	tenantID roachpb.TenantID,
	id int64,
	cfg *admissionpb.ResourceGroupConfig,
	tombstone bool,
	_ error,
) {
	keyTypes := []*types.T{d.columns[0].GetType(), d.columns[1].GetType()}
	keyVals := make([]rowenc.EncDatum, 2)
	if _, err := rowenc.DecodeIndexKey(keys.SystemSQLCodec, keyVals, nil, kv.Key); err != nil {
		return roachpb.TenantID{}, 0, nil, false, errors.Wrap(err, "decode tenant_resource_groups key")
	}
	for i := range keyVals {
		if err := keyVals[i].EnsureDecoded(keyTypes[i], &d.alloc); err != nil {
			return roachpb.TenantID{}, 0, nil, false, err
		}
	}
	tid, err := roachpb.MakeTenantID(uint64(tree.MustBeDInt(keyVals[0].Datum)))
	if err != nil {
		return roachpb.TenantID{}, 0, nil, false, errors.Wrap(err, "invalid tenant id")
	}
	id = int64(tree.MustBeDInt(keyVals[1].Datum))

	if !kv.Value.IsPresent() {
		return tid, id, nil, true, nil
	}
	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return roachpb.TenantID{}, 0, nil, false, errors.Wrap(err, "get tuple")
	}
	datums, err := d.decoder.Decode(&d.alloc, bytes)
	if err != nil {
		return roachpb.TenantID{}, 0, nil, false, errors.Wrap(err, "decode tuple")
	}
	cfgBytes := []byte(tree.MustBeDBytes(datums[3]))
	var c admissionpb.ResourceGroupConfig
	if err := protoutil.Unmarshal(cfgBytes, &c); err != nil {
		return roachpb.TenantID{}, 0, nil, false, errors.Wrapf(err, "unmarshal config (tenant=%d id=%d)", tid.ToUint64(), id)
	}
	return tid, id, &c, false, nil
}
