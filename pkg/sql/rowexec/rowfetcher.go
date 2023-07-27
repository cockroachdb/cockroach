// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rowexec

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// rowFetcher is an interface used to abstract a row.Fetcher so that a stat
// collector wrapper can be plugged in.
type rowFetcher interface {
	StartScan(
		_ context.Context, _ roachpb.Spans, spanIDs []int,
		batchBytesLimit rowinfra.BytesLimit, rowLimitHint rowinfra.RowLimit,
	) error
	StartInconsistentScan(
		_ context.Context,
		_ *kv.DB,
		initialTimestamp hlc.Timestamp,
		maxTimestampAge time.Duration,
		spans roachpb.Spans,
		batchBytesLimit rowinfra.BytesLimit,
		rowLimitHint rowinfra.RowLimit,
		qualityOfService sessiondatapb.QoSLevel,
	) error

	NextRow(ctx context.Context) (_ rowenc.EncDatumRow, spanID int, _ error)
	NextRowInto(
		ctx context.Context, destination rowenc.EncDatumRow, colIdxMap catalog.TableColMap,
	) (ok bool, err error)

	Reset()
	GetBytesRead() int64
	GetBatchRequestsIssued() int64
	// Close releases any resources held by this fetcher.
	Close(ctx context.Context)
}
