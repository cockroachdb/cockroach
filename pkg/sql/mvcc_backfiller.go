// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/errors"
)

// Merge merges the entries in the provide span sourceSpan from the index with
// sourceID into the index with destinationID.
func (sc *SchemaChanger) Merge(
	ctx context.Context,
	codec keys.SQLCodec,
	table catalog.TableDescriptor,
	sourceID descpb.IndexID,
	destinationID descpb.IndexID,
	sourceSpan roachpb.Span,
) error {
	sourcePrefix := rowenc.MakeIndexKeyPrefix(codec, table.GetID(), sourceID)
	prefixLen := len(sourcePrefix)
	destPrefix := rowenc.MakeIndexKeyPrefix(codec, table.GetID(), destinationID)

	const pageSize = 1000
	key := sourceSpan.Key
	destKey := make([]byte, len(destPrefix))

	for key != nil {
		err := sc.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// For now just grab all of the destination KVs and merge the corresponding entries.
			kvs, err := txn.Scan(ctx, key, sourceSpan.EndKey, int64(pageSize))
			if err != nil {
				return err
			}

			if len(kvs) == 0 {
				key = nil
				return nil
			}

			destKeys := make([]roachpb.Key, len(kvs))
			for i := range kvs {
				sourceKV := &kvs[i]

				if len(sourceKV.Key) < prefixLen {
					return errors.Errorf("Key for index entry %v does not start with prefix %v", sourceKV, sourceSpan.Key)
				}

				destKey = destKey[:0]
				destKey = append(destKey, destPrefix...)
				destKey = append(destKey, sourceKV.Key[prefixLen:]...)
				destKeys[i] = make([]byte, len(destKey))
				copy(destKeys[i], destKey)
			}

			wb := txn.NewBatch()
			for i := range kvs {
				mergedEntry, deleted, err := mergeEntry(&kvs[i], destKeys[i])
				if err != nil {
					return err
				}

				// We can blindly put and delete values during the merge since any
				// uniqueness variations in the merged index will be caught by
				// ValidateForwardIndexes and ValidateInvertedIndexes during validation.
				if deleted {
					wb.Del(mergedEntry.Key)
				} else {
					wb.Put(mergedEntry.Key, mergedEntry.Value)
				}
			}

			if err := txn.Run(ctx, wb); err != nil {
				return err
			}

			key = kvs[len(kvs)-1].Key.Next()
			return nil
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func mergeEntry(sourceKV *kv.KeyValue, destKey roachpb.Key) (*kv.KeyValue, bool, error) {
	var destTagAndData []byte
	var deleted bool

	tempWrapper, err := rowenc.DecodeWrapper(sourceKV.Value)
	if err != nil {
		return nil, false, err
	}

	if tempWrapper.Deleted {
		deleted = true
	} else {
		destTagAndData = tempWrapper.Value
	}

	value := &roachpb.Value{}
	value.SetTagAndData(destTagAndData)

	return &kv.KeyValue{
		Key:   destKey,
		Value: value,
	}, deleted, nil
}
