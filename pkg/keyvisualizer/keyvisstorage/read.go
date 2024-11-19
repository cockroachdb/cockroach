// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvisstorage

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// MostRecentSampleTime returns the timestamp of the most recent sample that has been persisted.
func MostRecentSampleTime(ctx context.Context, ie *sql.InternalExecutor) (time.Time, error) {

	rows, err := ie.QueryBufferedEx(
		ctx,
		"query-samples",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		"SELECT max(sample_time) FROM system.span_stats_samples",
	)

	if err != nil {
		return timeutil.UnixEpoch, err
	}

	sampleTime := rows[0][0]

	if sampleTime == tree.DNull {
		return timeutil.UnixEpoch, nil
	}

	return tree.MustBeDTimestamp(sampleTime).Time, nil
}

// ReadSamples returns all collected samples, formatted for consumption by the browser.
func ReadSamples(
	ctx context.Context, ie *sql.InternalExecutor,
) ([]serverpb.KeyVisSamplesResponse_KeyVisSample, error) {

	// dictionary to access a sample by its sample id
	samples := make(map[string]*serverpb.KeyVisSamplesResponse_KeyVisSample)

	sampleRows, err := ie.QueryBufferedEx(
		ctx,
		"query-samples",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		"SELECT * FROM system.span_stats_samples",
	)
	if err != nil {
		return nil, err
	}

	for _, row := range sampleRows {
		sampleID := tree.MustBeDUuid(row[0]).UUID.String()
		sampleTime := tree.MustBeDTimestamp(row[1])
		var buckets []serverpb.KeyVisSamplesResponse_Bucket
		samples[sampleID] = &serverpb.KeyVisSamplesResponse_KeyVisSample{
			Timestamp: sampleTime.Time,
			Buckets:   buckets,
		}
	}

	bucketRows, err := ie.QueryBufferedEx(
		ctx,
		"query-samples",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		"SELECT * FROM system.span_stats_buckets",
	)
	if err != nil {
		return nil, err
	}

	for _, row := range bucketRows {
		sampleID := tree.MustBeDUuid(row[1]).UUID.String()
		startKeyID := tree.MustBeDUuid(row[2]).UUID
		endKeyID := tree.MustBeDUuid(row[3]).UUID
		requests := tree.MustBeDInt(row[4])

		// create a bucket
		bucket := serverpb.KeyVisSamplesResponse_Bucket{
			StartKeyID: startKeyID,
			EndKeyID:   endKeyID,
			Requests:   uint64(requests),
		}

		samples[sampleID].Buckets = append(samples[sampleID].Buckets, bucket)
	}

	res := make([]serverpb.KeyVisSamplesResponse_KeyVisSample, 0, len(samples))

	for _, sample := range samples {
		res = append(res, *sample)
	}

	return res, nil
}

// ReadKeys returns the unique keys throughout all collected samples.
func ReadKeys(ctx context.Context, ie *sql.InternalExecutor) (map[string]roachpb.Key, error) {

	rows, err := ie.QueryBufferedEx(
		ctx,
		"query-unique-keys",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		"SELECT * FROM system.span_stats_unique_keys",
	)

	if err != nil {
		return nil, err
	}

	keys := make(map[string]roachpb.Key)

	for _, row := range rows {
		keyUUID := hex.EncodeToString(tree.MustBeDUuid(row[0]).UUID.GetBytes())
		keyBytes := tree.MustBeDBytes(row[1])
		keys[keyUUID] = roachpb.Key(keyBytes)
	}

	return keys, nil

}
