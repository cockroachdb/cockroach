// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantsettingswatcher

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// RowDecoder decodes rows from the tenant_settings table.
type RowDecoder struct {
	alloc   tree.DatumAlloc
	columns []catalog.Column
	decoder valueside.Decoder
}

var allTenantOverridesID = roachpb.TenantID{InternalValue: 0}

// MakeRowDecoder makes a new RowDecoder for the settings table.
func MakeRowDecoder() RowDecoder {
	columns := systemschema.TenantSettingsTable.PublicColumns()
	return RowDecoder{
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
	}
}

// DecodeRow decodes a row of the system.tenant_settings table. If the value is
// not present, TenantSetting.Value will be empty and the tombstone bool will be
// set.
func (d *RowDecoder) DecodeRow(
	kv roachpb.KeyValue,
) (_ roachpb.TenantID, _ roachpb.TenantSetting, tombstone bool, _ error) {
	// First we need to decode the setting name field from the index key.
	keyTypes := []*types.T{d.columns[0].GetType(), d.columns[1].GetType()}
	keyVals := make([]rowenc.EncDatum, 2)
	_, _, err := rowenc.DecodeIndexKey(keys.SystemSQLCodec, keyTypes, keyVals, nil, kv.Key)
	if err != nil {
		return roachpb.TenantID{}, roachpb.TenantSetting{}, false, errors.Wrap(err, "failed to decode key")
	}
	for i := range keyVals {
		if err := keyVals[i].EnsureDecoded(keyTypes[i], &d.alloc); err != nil {
			return roachpb.TenantID{}, roachpb.TenantSetting{}, false, err
		}
	}
	// We do not use MakeTenantID because we want to tolerate the 0 value.
	tenantID := roachpb.TenantID{InternalValue: uint64(tree.MustBeDInt(keyVals[0].Datum))}
	var setting roachpb.TenantSetting
	setting.Name = string(tree.MustBeDString(keyVals[1].Datum))
	if !kv.Value.IsPresent() {
		return tenantID, setting, true, nil
	}

	// The rest of the columns are stored as a family.
	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return roachpb.TenantID{}, roachpb.TenantSetting{}, false, err
	}

	datums, err := d.decoder.Decode(&d.alloc, bytes)
	if err != nil {
		return roachpb.TenantID{}, roachpb.TenantSetting{}, false, err
	}

	if value := datums[2]; value != tree.DNull {
		setting.Value.Value = string(tree.MustBeDString(value))
	}
	if typ := datums[4]; typ != tree.DNull {
		setting.Value.Type = string(tree.MustBeDString(typ))
	} else {
		// Column valueType is missing; default it to "s".
		setting.Value.Type = "s"
	}

	return tenantID, setting, false, nil
}
