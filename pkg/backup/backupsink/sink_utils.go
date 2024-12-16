// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupsink

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

func elideMVCCKeyPrefix(
	key storage.MVCCKey, mode execinfrapb.ElidePrefix,
) (storage.MVCCKey, []byte, error) {
	prefix, err := ElidedPrefix(key.Key, mode)
	if err != nil {
		return storage.MVCCKey{}, nil, err
	}
	cutKey, ok := bytes.CutPrefix(key.Key, prefix)
	if !ok {
		return storage.MVCCKey{}, nil, errors.AssertionFailedf("prefix mismatch %q does not have %q", key.Key, prefix)
	}
	key.Key = cutKey
	return key, prefix, nil
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

func generateUniqueSSTName(nodeID base.SQLInstanceID) string {
	// The data/ prefix, including a /, is intended to group SSTs in most of the
	// common file/bucket browse UIs.
	return fmt.Sprintf("data/%d.sst",
		builtins.GenerateUniqueInt(builtins.ProcessUniqueID(nodeID)))
}

// sameAsFileSpan returns true if the span, start, and end match the span, start, and end of the BackupManifest_File.
func sameAsFileSpan(
	span roachpb.Span, start, end hlc.Timestamp, backupFile *backuppb.BackupManifest_File,
) bool {
	return backupFile.Span.Equal(span) && backupFile.StartTime.Equal(start) && backupFile.EndTime.Equal(end)
}

// extendsFileSpan returns true if a span can successfully contiguously extend the span of a BackupManifest_File.
func extendsFileSpan(
	span roachpb.Span, start, end hlc.Timestamp, backupFile *backuppb.BackupManifest_File,
) bool {
	return backupFile.Span.EndKey.Equal(span.Key) &&
		backupFile.StartTime.Equal(start) &&
		backupFile.EndTime.Equal(end)
}
