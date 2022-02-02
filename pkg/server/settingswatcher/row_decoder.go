// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settingswatcher

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// RowDecoder decodes rows from the settings table.
type RowDecoder struct {
	codec   keys.SQLCodec
	alloc   tree.DatumAlloc
	columns []catalog.Column
	decoder valueside.Decoder
}

// MakeRowDecoder makes a new RowDecoder for the settings table.
func MakeRowDecoder(codec keys.SQLCodec) RowDecoder {
	columns := systemschema.SettingsTable.PublicColumns()
	return RowDecoder{
		codec:   codec,
		columns: columns,
		decoder: valueside.MakeDecoder(columns),
	}
}

// DecodeRow decodes a row of the system.settings table. If the value is not
// present, the setting key will be returned but the value will be zero and the
// tombstone bool will be set.
func (d *RowDecoder) DecodeRow(
	kv roachpb.KeyValue,
) (setting string, val settings.EncodedValue, tombstone bool, _ error) {
	// First we need to decode the setting name field from the index key.
	{
		types := []*types.T{d.columns[0].GetType()}
		nameRow := make([]rowenc.EncDatum, 1)
		_, _, err := rowenc.DecodeIndexKey(d.codec, types, nameRow, nil, kv.Key)
		if err != nil {
			return "", settings.EncodedValue{}, false, errors.Wrap(err, "failed to decode key")
		}
		if err := nameRow[0].EnsureDecoded(types[0], &d.alloc); err != nil {
			return "", settings.EncodedValue{}, false, err
		}
		setting = string(tree.MustBeDString(nameRow[0].Datum))
	}
	if !kv.Value.IsPresent() {
		return setting, settings.EncodedValue{}, true, nil
	}

	// The rest of the columns are stored as a family.
	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return "", settings.EncodedValue{}, false, err
	}

	datums, err := d.decoder.Decode(&d.alloc, bytes)
	if err != nil {
		return "", settings.EncodedValue{}, false, err
	}

	if value := datums[1]; value != tree.DNull {
		val.Value = string(tree.MustBeDString(value))
	}
	if typ := datums[3]; typ != tree.DNull {
		val.Type = string(tree.MustBeDString(typ))
	} else {
		// Column valueType is missing; default it to "s".
		val.Type = "s"
	}

	return setting, val, false, nil
}
