// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package util

import (
	"hash/crc32"

	"github.com/cockroachdb/errors"
)

// CRC32 computes the Castagnoli CRC32 of the given data.
func CRC32(data []byte) uint32 {
	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	if _, err := hash.Write(data); err != nil {
		panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
	}
	return hash.Sum32()
}
