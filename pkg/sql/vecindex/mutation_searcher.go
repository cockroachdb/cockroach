// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecindex

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/cspann"
	"github.com/cockroachdb/cockroach/pkg/sql/vecindex/vecstore"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

// MutationSearcher is a helper struct that makes it easy for the execution
// engine to search for mutation partitions (e.g. the partition where an insert
// or delete operation runs). It wraps creation of a C-SPANN transaction, setup
// of search context and parameters, and offers methods to search.
//
// NOTE: MutationSearcher is intended to be embedded within an execution engine
// processor object.
type MutationSearcher struct {
	idx          *cspann.Index
	txn          vecstore.Txn
	idxCtx       cspann.Context
	partitionKey tree.Datum
	encoded      tree.Datum
}

// Init wraps the given KV transaction in a C-SPANN transaction and initializes
// the search context.
//
// NOTE: The index is expected to come from a call to Manager.Get, and therefore
// using a vecstore.Store instance.
func (s *MutationSearcher) Init(idx *cspann.Index, txn *kv.Txn) {
	s.idx = idx
	s.txn.Init(idx.Store().(*vecstore.Store), txn)
	s.idxCtx.Init(&s.txn)

	// If the index is deterministic, then synchronously run the background worker
	// to process any pending fixups.
	if s.idx.Options().IsDeterministic {
		s.idx.ProcessFixups()
	}
}

// SearchForInsert triggers a search for the partition in which to insert the
// input vector. The partition's key is returned by PartitionKey() and the
// input vector's quantized and encoded form is returned by EncodedVector().
func (s *MutationSearcher) SearchForInsert(
	ctx context.Context, prefix roachpb.Key, vec vector.T,
) error {
	res, err := s.idx.SearchForInsert(ctx, &s.idxCtx, cspann.TreeKey(prefix), vec)
	if err != nil {
		return err
	}

	// NOTE: The insert partition's centroid vector is already randomized, so
	// only need to get the randomized input vector.
	partitionKey := res.ChildKey.PartitionKey
	s.partitionKey = tree.NewDInt(tree.DInt(partitionKey))
	centroid := res.Vector
	randomizedVec := s.idxCtx.RandomizedVector()

	// Quantize and encode the input vector with respect to the insert partition's
	// centroid.
	quantizedVec, err := s.txn.QuantizeAndEncode(partitionKey, centroid, randomizedVec)
	if err != nil {
		return err
	}
	s.encoded = tree.NewDBytes(tree.DBytes(quantizedVec))
	return nil
}

// SearchForDelete triggers a search for the partition which contains the vector
// to be deleted, identified by its primary key. If the input vector is found,
// its partition is returned by PartitionKey().
func (s *MutationSearcher) SearchForDelete(
	ctx context.Context, prefix roachpb.Key, vec vector.T, key cspann.KeyBytes,
) error {
	res, err := s.idx.SearchForDelete(ctx, &s.idxCtx, cspann.TreeKey(prefix), vec, key)
	if err != nil {
		return err
	}
	if res != nil {
		s.partitionKey = tree.NewDInt(tree.DInt(res.ParentPartitionKey))
	} else {
		s.partitionKey = tree.DNull
	}

	// No EncodedVector for the Delete case.
	s.encoded = nil
	return nil
}

// PartitionKey returns the key of the partition where an insertion or deletion
// will occur, as a datum value.
// NOTE: This is set to tree.DNull in the SearchForDelete() case, if the vector
// cannot be found.
func (s *MutationSearcher) PartitionKey() tree.Datum {
	return s.partitionKey
}

// EncodedVector returns the encoded form of the insertion vector that has been
// quantized with respect to the partition in which it will be inserted.
// NOTE: This is set to nil in the SearchForDelete() case.
func (s *MutationSearcher) EncodedVector() tree.Datum {
	return s.encoded
}
