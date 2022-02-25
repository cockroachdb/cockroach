// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

// IndexFetchSpecVersionInitial is the initial IndexFetchSpec version.
const IndexFetchSpecVersionInitial = 1

// KeyColumns returns the key columns in the index, excluding any key suffix
// columns.
func (s *IndexFetchSpec) KeyColumns() []IndexFetchSpec_KeyColumn {
	return s.KeyAndSuffixColumns[:len(s.KeyAndSuffixColumns)-int(s.NumKeySuffixColumns)]
}

// KeyFullColumns returns the key columns in the index, plus all key suffix
// columns if that index is not a unique index. It parallels
// TableDescriptor.IndexFullColumns.
func (s *IndexFetchSpec) KeyFullColumns() []IndexFetchSpec_KeyColumn {
	if s.IsUniqueIndex {
		// For unique indexes, the suffix columns are not part of the key (except
		// when the key columns contain a NULL).
		return s.KeyColumns()
	}
	return s.KeyAndSuffixColumns
}

// KeySuffixColumns returns the key suffix columns.
func (s *IndexFetchSpec) KeySuffixColumns() []IndexFetchSpec_KeyColumn {
	return s.KeyAndSuffixColumns[len(s.KeyAndSuffixColumns)-int(s.NumKeySuffixColumns):]
}
