// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keyvisstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func MostRecentSampleTime(ctx context.Context, ie *sql.InternalExecutor) (*hlc.Timestamp, error) {

	rows, err := ie.QueryBufferedEx(
		ctx,
		"query-samples",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		"SELECT MAX(sample_time) FROM system.span_stats_samples",
	)

	if err != nil {
		return nil, err
	}

	sampleTime := rows[0][0]

	if sampleTime == tree.DNull {
		return &hlc.Timestamp{}, nil
	}

	return &hlc.Timestamp{
		WallTime: tree.MustBeDTimestamp(sampleTime).UnixNano(),
	}, nil
}

// ReadSamples returns all collected samples, formatted for consumption by the browser.
func ReadSamples(
	ctx context.Context, ie *sql.InternalExecutor,
) ([]*serverpb.KeyVisSamplesResponse_KeyVisSample, error) {

	// dictionary to access a sample by its sample id
	samples := make(map[string]*serverpb.KeyVisSamplesResponse_KeyVisSample)

	sampleRows, err := ie.QueryBufferedEx(
		ctx,
		"query-samples",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		"SELECT * FROM system.span_stats_samples",
	)
	if err != nil {
		return nil, err
	}

	for _, row := range sampleRows {
		sampleID := tree.MustBeDUuid(row[0]).UUID.String()
		sampleTime := tree.MustBeDTimestamp(row[1])
		samples[sampleID] = &serverpb.KeyVisSamplesResponse_KeyVisSample{
			Timestamp: hlc.Timestamp{WallTime: sampleTime.UnixNano()},
			Buckets:   make([]*serverpb.KeyVisSamplesResponse_Bucket, 0),
		}
	}

	bucketRows, err := ie.QueryBufferedEx(
		ctx,
		"query-samples",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		"SELECT * FROM system.span_stats_buckets",
	)
	if err != nil {
		return nil, err
	}

	for _, row := range bucketRows {
		sampleID := tree.MustBeDUuid(row[1]).UUID.String()
		startKeyID := tree.MustBeDUuid(row[2]).UUID.String()
		endKeyID := tree.MustBeDUuid(row[3]).UUID.String()
		requests := tree.MustBeDInt(row[4])

		// create a bucket
		bucket := &serverpb.KeyVisSamplesResponse_Bucket{
			StartKeyId: startKeyID,
			EndKeyId:   endKeyID,
			Requests:   uint64(requests),
		}

		sample := samples[sampleID]
		sample.Buckets = append(sample.Buckets, bucket)
	}

	res := make([]*serverpb.KeyVisSamplesResponse_KeyVisSample, 0)

	for _, sample := range samples {
		res = append(res, sample)
	}

	return res, nil
}

// ReadKeys returns the unique keys throughout all collected samples.
func ReadKeys(ctx context.Context, ie *sql.InternalExecutor) (map[string]roachpb.Key, error) {

	rows, err := ie.QueryBufferedEx(
		ctx,
		"query-unique-keys",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		"SELECT * FROM system.span_stats_unique_keys",
	)

	if err != nil {
		return nil, err
	}

	keys := make(map[string]roachpb.Key)

	for _, row := range rows {
		keyUUID := tree.MustBeDUuid(row[0]).UUID.String()
		keyBytes := tree.MustBeDBytes(row[1])
		keys[keyUUID] = roachpb.Key(keyBytes)
	}

	return keys, nil

}
