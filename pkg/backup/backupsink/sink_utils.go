// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backupsink

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

// ElidedPrefix returns the prefix of the key that is elided by the given mode.
func ElidedPrefix(key roachpb.Key, mode execinfrapb.ElidePrefix) ([]byte, error) {
	switch mode {
	case execinfrapb.ElidePrefix_TenantAndTable:
		rest, err := keys.StripTablePrefix(key)
		if err != nil {
			return nil, err
		}
		return key[: len(key)-len(rest) : len(key)-len(rest)], nil

	case execinfrapb.ElidePrefix_Tenant:
		rest, err := keys.StripTenantPrefix(key)
		if err != nil {
			return nil, err
		}
		return key[: len(key)-len(rest) : len(key)-len(rest)], nil
	}
	return nil, nil
}

// adjustFileEndKey checks if the export respsonse end key can be used as a
// split point during restore. If the end key is not splitable (i.e. it splits
// two column families in the same row), the function will attempt to adjust the
// endkey to become splitable. The function returns the potentially adjusted
// end key and whether this end key is mid row/unsplitable (i.e. splits a 2
// column families or mvcc versions).
func adjustFileEndKey(endKey, maxPointKey, maxRangeEnd roachpb.Key) (roachpb.Key, bool) {
	maxKey := maxPointKey
	if maxKey.Compare(maxRangeEnd) < 0 {
		maxKey = maxRangeEnd
	}

	endRowKey, err := keys.EnsureSafeSplitKey(endKey)
	if err != nil {
		// If the key does not parse a family key, it must be from reaching the end
		// of a range and be a range boundary.
		return endKey, false
	}

	// If the end key parses as a family key but truncating to the row key does
	// _not_ produce a row key greater than every key in the file, then one of two
	// things has happened: we *did* stop at family key mid-row, so we copied some
	// families after the row key but have more to get in the next file -- so we
	// must *not* flush now -- or the file ended at a range boundary that _looks_
	// like a family key due to a numeric suffix, so the (nonsense) truncated key
	// is now some prefix less than the last copied key. The latter is unfortunate
	// but should be rare given range-sized export requests.
	if endRowKey.Compare(maxKey) <= 0 {
		return endKey, true
	}

	// If the file end does parse as a family key but the truncated 'row' key is
	// still above any key in the file, the end key likely came from export's
	// iteration stopping early and setting the end to the resume key, i.e. the
	// next real family key. In this case, we are not mid-row, but want to adjust
	// our span end -- and where we resume the next file -- to be this row key.
	// Thus return the truncated row key and false.
	return endRowKey, false

}

func (s *fileSSTSink) copyPointKeys(ctx context.Context, dataSST []byte) (roachpb.Key, error) {
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := storage.NewMemSSTIterator(dataSST, false, iterOpts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var valueBuf []byte

	empty := true
	for iter.SeekGE(storage.MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if err := s.pacer.Pace(ctx); err != nil {
			return nil, err
		}
		if valid, err := iter.Valid(); !valid || err != nil {
			if err != nil {
				return nil, err
			}
			break
		}
		k := iter.UnsafeKey()
		suffix, ok := bytes.CutPrefix(k.Key, s.elidePrefix)
		if !ok {
			return nil, errors.AssertionFailedf("prefix mismatch %q does not have %q", k.Key, s.elidePrefix)
		}
		k.Key = suffix

		raw, err := iter.UnsafeValue()
		if err != nil {
			return nil, err
		}

		valueBuf = append(valueBuf[:0], raw...)
		v, err := storage.DecodeValueFromMVCCValue(valueBuf)
		if err != nil {
			return nil, errors.Wrapf(err, "decoding mvcc value %s", k)
		}

		// Checksums include the key, but *exported* keys no longer live at that key
		// once they are exported, and could be restored as some other key, so zero
		// out the checksum.
		v.ClearChecksum()

		// NB: DecodeValueFromMVCCValue does not decode the MVCCValueHeader, which
		// we need to back up. In other words, if we passed v.RawBytes to the put
		// call below, we would lose data. By putting valueBuf, we pass the value
		// header and the cleared checksum.
		//
		// TODO(msbutler): create a ClearChecksum() method that can act on raw value
		// bytes, and remove this hacky code.
		if k.Timestamp.IsEmpty() {
			if err := s.sst.PutUnversioned(k.Key, valueBuf); err != nil {
				return nil, err
			}
		} else {
			if err := s.sst.PutRawMVCC(k, valueBuf); err != nil {
				return nil, err
			}
		}
		empty = false
	}
	if empty {
		return nil, nil
	}
	iter.Prev()
	ok, err := iter.Valid()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.AssertionFailedf("failed to find last key of non-empty file")
	}
	return iter.UnsafeKey().Key.Clone(), nil
}

// copyRangeKeys copies all range keys from the dataSST into the buffer and
// returns the max range key observed.
func (s *fileSSTSink) copyRangeKeys(dataSST []byte) (roachpb.Key, error) {
	iterOpts := storage.IterOptions{
		KeyTypes:   storage.IterKeyTypeRangesOnly,
		LowerBound: keys.LocalMax,
		UpperBound: keys.MaxKey,
	}
	iter, err := storage.NewMemSSTIterator(dataSST, false, iterOpts)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var maxKey roachpb.Key
	for iter.SeekGE(storage.MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return nil, err
		} else if !ok {
			break
		}
		rangeKeys := iter.RangeKeys()
		for _, v := range rangeKeys.Versions {
			rk := rangeKeys.AsRangeKey(v)
			if rk.EndKey.Compare(maxKey) > 0 {
				maxKey = append(maxKey[:0], rk.EndKey...)
			}
			var ok bool
			if rk.StartKey, ok = bytes.CutPrefix(rk.StartKey, s.elidePrefix); !ok {
				return nil, errors.AssertionFailedf("prefix mismatch %q does not have %q", rk.StartKey, s.elidePrefix)
			}
			if rk.EndKey, ok = bytes.CutPrefix(rk.EndKey, s.elidePrefix); !ok {
				return nil, errors.AssertionFailedf("prefix mismatch %q does not have %q", rk.EndKey, s.elidePrefix)
			}
			if err := s.sst.PutRawMVCCRangeKey(rk, v.Value); err != nil {
				return nil, err
			}
		}
	}
	return maxKey, nil
}

func generateUniqueSSTName(nodeID base.SQLInstanceID) string {
	// The data/ prefix, including a /, is intended to group SSTs in most of the
	// common file/bucket browse UIs.
	return fmt.Sprintf("data/%d.sst",
		builtins.GenerateUniqueInt(builtins.ProcessUniqueID(nodeID)))
}
