// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// zonesDecoder decodes the zone ID (primary key) of rows from system.zones.
// It's not safe for concurrent use.
type zonesDecoder struct {
	alloc rowenc.DatumAlloc
	codec keys.SQLCodec
}

// NewZonesDecoder instantiates a zonesDecoder.
func NewZonesDecoder(codec keys.SQLCodec) *zonesDecoder {
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
	_, matches, _, err := rowenc.DecodeIndexKey(
		zd.codec, tbl, tbl.GetPrimaryIndex(),
		types, startKeyRow, nil, key,
	)
	if err != nil || !matches {
		return descpb.InvalidID, errors.Newf("failed to decode key in system.zones %v", key)
	}
	if err := startKeyRow[0].EnsureDecoded(types[0], &zd.alloc); err != nil {
		return descpb.InvalidID, errors.Newf("failed to decode key in system.zones %v", key)
	}
	descID := descpb.ID(tree.MustBeDInt(startKeyRow[0].Datum))
	return descID, nil
}
