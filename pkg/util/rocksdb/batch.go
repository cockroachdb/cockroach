// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rocksdb

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

const (
	// The batch header is composed of an 8-byte sequence number (all zeroes) and
	// 4-byte count of the number of entries in the batch.
	headerSize int = 12
	countPos       = 8
)

// BatchCount provides an efficient way to get the count of mutations in a
// RocksDB Batch representation.
func BatchCount(repr []byte) (int, error) {
	if len(repr) < headerSize {
		return 0, errors.Errorf("batch repr too small: %d < %d", len(repr), headerSize)
	}
	return int(binary.LittleEndian.Uint32(repr[countPos:headerSize])), nil
}

// BatchMerge ...
func BatchMerge(a, b []byte) ([]byte, error) {
	if a == nil {
		return b, nil
	}
	if b == nil {
		return a, nil
	}

	aCount, err := BatchCount(a)
	if err != nil {
		return nil, err
	}
	if aCount == 0 {
		return b, nil
	}

	bCount, err := BatchCount(b)
	if err != nil {
		return nil, err
	}
	if bCount == 0 {
		return a, nil
	}

	a = append(a, b[headerSize:]...)
	v := uint32(aCount + bCount)
	binary.LittleEndian.PutUint32(a[countPos:headerSize], v)
	return a, nil
}
