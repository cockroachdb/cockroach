// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfigsqlwatcher

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/errors"
)

// zonesDecoder decodes the zone ID (primary key) of rows from system.zones.
// It's not safe for concurrent use.
type zonesDecoder struct {
	alloc tree.DatumAlloc
	codec keys.SQLCodec
}

// newZonesDecoder instantiates a zonesDecoder.
func newZonesDecoder(codec keys.SQLCodec) *zonesDecoder {
	return &zonesDecoder{
		codec: codec,
	}
}

// DecodePrimaryKey decodes the primary key (zone ID) from the system.zones
// table.
func (zd *zonesDecoder) DecodePrimaryKey(key roachpb.Key) (descpb.ID, error) {
	// Decode the descriptor ID from the key.
	tbl := systemschema.ZonesTable
	types := []*types.T{tbl.PublicColumns()[0].GetType()}
	startKeyRow := make([]rowenc.EncDatum, 1)
	if _, err := rowenc.DecodeIndexKey(zd.codec, startKeyRow, nil /* colDirs */, key); err != nil {
		return descpb.InvalidID, errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode key in system.zones %v %v", key, debugutil.Stack())
	}
	if err := startKeyRow[0].EnsureDecoded(types[0], &zd.alloc); err != nil {
		return descpb.InvalidID, errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode key in system.zones %v", key)
	}
	descID := descpb.ID(tree.MustBeDInt(startKeyRow[0].Datum))
	return descID, nil
}

// TestingZonesDecoderDecodePrimaryKey constructs a zonesDecoder using the given
// codec and decodes the supplied key using it. This wrapper is exported for
// testing purposes to ensure the struct remains private.
func TestingZonesDecoderDecodePrimaryKey(codec keys.SQLCodec, key roachpb.Key) (descpb.ID, error) {
	return newZonesDecoder(codec).DecodePrimaryKey(key)
}
