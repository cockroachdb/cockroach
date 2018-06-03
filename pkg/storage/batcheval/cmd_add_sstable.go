// Copyright 2018 The Cockroach Authors.
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

package batcheval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/golang/leveldb/table"
)

// MakeAddSSTableResult constructs the result of an AddSSTable command from the
// raw bytes of the SST. The full implementation of the AddSSTable command is
// CCL licensed, in storageccl, and performs additional verification and stats
// computation. This piece is part of the OSS edition for testing.
func MakeAddSSTableResult(
	ctx context.Context, data []byte, sst *table.Reader,
) (result.Result, error) {
	globalSeqno, ok := sst.Properties["rocksdb.external_sst_file.global_seqno"]
	if !ok {
		return result.Result{}, fmt.Errorf("sst missing global seqno property")
	}
	return result.Result{
		Replicated: storagebase.ReplicatedEvalResult{
			AddSSTable: &storagebase.ReplicatedEvalResult_AddSSTable{
				Data:              data,
				CRC32:             util.CRC32(data),
				GlobalSeqnoOffset: globalSeqno.Offset,
			},
		},
	}, nil
}
