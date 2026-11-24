// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/sqlwriter"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangecache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/errors"
)

// nodeRouter routes a transaction's write set to the KV node that holds the
// leaseholder for a sampled row's primary key range. It encapsulates destination
// key encoding and range cache lookups.
type nodeRouter struct {
	encoders   map[descpb.ID]*destKeyEncoder
	rangeCache *rangecache.RangeCache
}

// newNodeRouter creates a nodeRouter by looking up destination table descriptors
// and initializing per-table key encoders.
func newNodeRouter(
	ctx context.Context,
	codec keys.SQLCodec,
	db descs.DB,
	schema execinfrapb.LDRSchema,
	rangeCache *rangecache.RangeCache,
) (*nodeRouter, error) {
	encoders, err := newDestKeyEncoders(ctx, codec, db, schema)
	if err != nil {
		return nil, err
	}
	return &nodeRouter{
		encoders:   encoders,
		rangeCache: rangeCache,
	}, nil
}

// RouteWriteSet picks a random row from the write set, encodes its destination
// primary key, and looks up the leaseholder node via the range cache. Returns
// the leaseholder's NodeID. On range cache lookup failure, returns 0 and an
// error so the caller can apply a fallback strategy.
func (r *nodeRouter) RouteWriteSet(
	ctx context.Context, writeSet []ldrdecoder.DecodedRow,
) (roachpb.NodeID, error) {
	ri, err := r.lookupRange(ctx, writeSet)
	if err != nil {
		return 0, err
	}
	return ri.Lease.Replica.NodeID, nil
}

// lookupRange picks a random row from the write set, encodes its destination
// primary key, and looks up the range via the range cache.
func (r *nodeRouter) lookupRange(
	ctx context.Context, writeSet []ldrdecoder.DecodedRow,
) (roachpb.RangeInfo, error) {
	if len(writeSet) == 0 {
		return roachpb.RangeInfo{}, errors.New("empty write set")
	}
	row := writeSet[rand.Intn(len(writeSet))]
	enc, ok := r.encoders[row.TableID]
	if !ok {
		return roachpb.RangeInfo{}, errors.AssertionFailedf(
			"no dest key encoder for table %d", row.TableID,
		)
	}
	rKey, err := enc.encodeKey(row)
	if err != nil {
		return roachpb.RangeInfo{}, err
	}
	ri, err := r.rangeCache.Lookup(ctx, rKey)
	if err != nil {
		return roachpb.RangeInfo{}, errors.Wrap(err, "range cache lookup")
	}
	return ri, nil
}

// destKeyEncoder holds per-table state for encoding a row's primary key for
// range cache lookups.
type destKeyEncoder struct {
	tableDesc catalog.TableDescriptor
	colMap    catalog.TableColMap
	keyPrefix []byte
}

// newDestKeyEncoders looks up destination table descriptors and builds a
// destKeyEncoder for each table in the schema. The colMap is built using
// sqlwriter.GetColumnSchema to match the datum ordering in DecodedRow.Row.
func newDestKeyEncoders(
	ctx context.Context, codec keys.SQLCodec, db descs.DB, schema execinfrapb.LDRSchema,
) (map[descpb.ID]*destKeyEncoder, error) {
	encoders := make(map[descpb.ID]*destKeyEncoder, len(schema.TableMetadataByDestID))
	err := db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		for rawDestID := range schema.TableMetadataByDestID {
			destID := descpb.ID(rawDestID)
			desc, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).
				WithoutNonPublic().Get().Table(ctx, destID)
			if err != nil {
				return err
			}
			var colMap catalog.TableColMap
			for i, col := range sqlwriter.GetColumnSchema(desc) {
				colMap.Set(col.Column.GetID(), i)
			}
			encoders[destID] = &destKeyEncoder{
				tableDesc: desc,
				colMap:    colMap,
				keyPrefix: rowenc.MakeIndexKeyPrefix(
					codec, destID, desc.GetPrimaryIndex().GetID(),
				),
			}
		}
		return nil
	})
	return encoders, err
}

// encodeKey encodes the row's primary key using the destination table
// descriptor. Returns an error if encoding fails.
func (enc *destKeyEncoder) encodeKey(row ldrdecoder.DecodedRow) (roachpb.RKey, error) {
	key, _, err := rowenc.EncodeIndexKey(
		enc.tableDesc,
		enc.tableDesc.GetPrimaryIndex(),
		enc.colMap,
		row.Row,
		enc.keyPrefix,
	)
	if err != nil {
		return roachpb.RKey{}, errors.Wrapf(
			err, "encoding dest key for table %d", row.TableID,
		)
	}
	return keys.Addr(key)
}
