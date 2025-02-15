// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/quantize"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann/workspace"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
	"github.com/cockroachdb/errors"
)

// Store implements the cspann.Store interface for KV backed vector indices.
type Store struct {
	db descs.DB // Used to generate new partition IDs

	// Used for generating prefixes and reading from the PK to get full length
	// vectors.
	codec   keys.SQLCodec
	tableID catid.DescID
	indexID catid.IndexID

	// The root partition always uses the UnQuantizer while other partitions may use
	// any quantizer.
	rootQuantizer quantize.Quantizer
	quantizer     quantize.Quantizer

	prefix    roachpb.Key            // KV prefix for the vector index.
	pkPrefix  roachpb.Key            // KV prefix for the primary key.
	fetchSpec fetchpb.IndexFetchSpec // A pre-built fetch spec for this index.
	colIdxMap catalog.TableColMap    // A column map for extracting full sized vectors from the PK
}

var _ cspann.Store = (*Store)(nil)

// NewWithColumnID creates a Store for an index on the provided table descriptor
// using the provided column ID as the vector column for the index. This is used
// in unit tests where full vector index creation capabilities aren't
// necessarily available.
func NewWithColumnID(
	db descs.DB,
	quantizer quantize.Quantizer,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	indexID catid.IndexID,
	vectorColumnID descpb.ColumnID,
) (ps *Store, err error) {
	ps = &Store{
		db:            db,
		codec:         codec,
		tableID:       tableDesc.GetID(),
		indexID:       indexID,
		rootQuantizer: quantize.NewUnQuantizer(quantizer.GetDims()),
		quantizer:     quantizer,
	}

	pk := tableDesc.GetPrimaryIndex()
	ps.prefix = rowenc.MakeIndexKeyPrefix(codec, tableDesc.GetID(), indexID)
	ps.pkPrefix = rowenc.MakeIndexKeyPrefix(codec, tableDesc.GetID(), pk.GetID())

	ps.colIdxMap.Set(vectorColumnID, 0)
	err = rowenc.InitIndexFetchSpec(
		&ps.fetchSpec,
		ps.codec,
		tableDesc,
		pk,
		[]descpb.ColumnID{vectorColumnID},
	)
	return ps, err
}

// New creates a cspann.Store interface backed by the KV for a single vector
// index.
func New(
	ctx context.Context,
	db descs.DB,
	quantizer quantize.Quantizer,
	codec keys.SQLCodec,
	tableID catid.DescID,
	indexID catid.IndexID,
) (ps *Store, err error) {
	var tableDesc catalog.TableDescriptor
	err = db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		var err error
		tableDesc, err = txn.Descriptors().ByIDWithLeased(txn.KV()).Get().Table(ctx, tableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	var index catalog.Index
	for _, desc := range tableDesc.DeletableNonPrimaryIndexes() {
		if desc.GetID() == indexID {
			index = desc
			break
		}
	}
	if index == nil {
		return nil, errors.AssertionFailedf("index %d not found in table %d", indexID, tableID)
	}

	vectorColumnID := index.VectorColumnID()

	return NewWithColumnID(db, quantizer, codec, tableDesc, indexID, vectorColumnID)
}

// BeginTransaction is part of the cspann.Store interface. Begin creates a new
// KV transaction on behalf of the user and prepares it to operate on the vector
// store.
func (s *Store) BeginTransaction(ctx context.Context) (cspann.Txn, error) {
	return newTxn(s, s.db.KV().NewTxn(ctx, "cspann.Store begin transaction")), nil
}

// CommitTransaction is part of the cspann.Store interface. Commit commits the
// underlying KV transaction wrapped by the cspann.Txn passed in.
func (s *Store) CommitTransaction(ctx context.Context, txn cspann.Txn) error {
	return txn.(*storeTxn).kv.Commit(ctx)
}

// AbortTransaction is part of the cspann.Store interface. Abort causes the
// underlying KV transaction wrapped by the passed cspann.Txn to roll back.
func (s *Store) AbortTransaction(ctx context.Context, txn cspann.Txn) error {
	return txn.(*storeTxn).kv.Rollback(ctx)
}

// MergeStats is part of the cspann.Store interface.
func (s *Store) MergeStats(ctx context.Context, stats *cspann.IndexStats, skipMerge bool) error {
	// TODO(mw5h): Implement MergeStats. We're not panicking here because some tested
	// functionality needs to call this function but does not depend on the results.
	return nil
}

// WrapTxn wraps the passed KV transaction in a vecstore.Txn. This allows vector
// search operations to be performed in an existing transaction.
func (s *Store) WrapTxn(txn *kv.Txn) cspann.Txn {
	return newTxn(s, txn)
}

// QuantizeAndEncode returns the quantized and encoded form of the given vector.
// It expects that the vector has already been randomized.
//
// TODO(drewk): move this to a wrapper Index struct. Ideally, this would also
// allow us to re-use an existing workspace.
func (s *Store) QuantizeAndEncode(
	partition cspann.PartitionKey, centroid, randomizedVec vector.T,
) (quantized []byte, err error) {
	// Quantize and encode the randomized vector.
	var quantizer quantize.Quantizer
	if partition == cspann.RootKey {
		quantizer = s.rootQuantizer
	} else {
		quantizer = s.quantizer
	}
	codec := newStoreCodec(quantizer)
	return codec.encodeVector(&workspace.T{}, randomizedVec, centroid)
}
