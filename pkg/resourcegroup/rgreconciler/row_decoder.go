// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rgreconciler

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/resourcegroup/rgpb"
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
// system.resource_groups into the (id, upsert, tombstone) tuple the
// reconciler operates on.
type rowDecoder struct {
	alloc   tree.DatumAlloc
	columns []catalog.Column
	decoder valueside.Decoder
}

func makeRowDecoder() rowDecoder {
	cols := systemschema.ResourceGroupsTable.PublicColumns()
	return rowDecoder{
		columns: cols,
		decoder: valueside.MakeDecoder(cols),
	}
}

// decode parses one KV from a rangefeed event. tombstone is true for
// delete events; in that case only id is meaningful and upsert is nil.
func (d *rowDecoder) decode(
	codec keys.SQLCodec, kv roachpb.KeyValue,
) (id int64, upsert *rgpb.ResourceGroupUpsert, tombstone bool, _ error) {
	keyTypes := []*types.T{d.columns[0].GetType()}
	keyVals := make([]rowenc.EncDatum, 1)
	if _, err := rowenc.DecodeIndexKey(codec, keyVals, nil, kv.Key); err != nil {
		return 0, nil, false, errors.Wrap(err, "decode resource_groups key")
	}
	if err := keyVals[0].EnsureDecoded(keyTypes[0], &d.alloc); err != nil {
		return 0, nil, false, err
	}
	id = int64(tree.MustBeDInt(keyVals[0].Datum))

	if !kv.Value.IsPresent() {
		return id, nil, true, nil
	}

	bytes, err := kv.Value.GetTuple()
	if err != nil {
		return 0, nil, false, errors.Wrap(err, "get tuple")
	}
	datums, err := d.decoder.Decode(&d.alloc, bytes)
	if err != nil {
		return 0, nil, false, errors.Wrap(err, "decode tuple")
	}
	name := string(tree.MustBeDString(datums[1]))
	cfgBytes := []byte(tree.MustBeDBytes(datums[2]))
	var cfg admissionpb.ResourceGroupConfig
	if err := protoutil.Unmarshal(cfgBytes, &cfg); err != nil {
		return 0, nil, false, errors.Wrapf(err, "unmarshal config (id=%d)", id)
	}
	return id, &rgpb.ResourceGroupUpsert{Id: id, Name: name, Config: cfg}, false, nil
}
