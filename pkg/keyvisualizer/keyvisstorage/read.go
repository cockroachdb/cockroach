package keyvisstorage

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"time"
)

func ReadSamples (
	ctx context.Context,
	ie *sql.InternalExecutor,
	) ([]*serverpb.KeyVisSamplesResponse_KeyVisSample, error) {

	// get samples and buckets
	// going to need to convert a sql timestamp to hlc.timestamp

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
		sampleID, err := datumToNative(row[0])
		if err != nil {
			return nil, err
		}
		sampleTime, err := datumToNative(row[1])
		if err != nil {
			return nil, err
		}

		samples[sampleID.(string)] = &serverpb.KeyVisSamplesResponse_KeyVisSample{
			Timestamp: hlc.Timestamp{WallTime: sampleTime.(time.Time).UnixNano()},
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
		sampleID, err := datumToNative(row[1])
		if err != nil {
			return nil, err
		}
		startKeyID, err := datumToNative(row[2])
		if err != nil {
			return nil, err
		}
		endKeyID, err := datumToNative(row[3])
		if err != nil {
			return nil, err
		}
		requests, err := datumToNative(row[4])
		if err != nil {
			return nil, err
		}

		// create a bucket
		bucket := &serverpb.KeyVisSamplesResponse_Bucket{
			StartKeyId: startKeyID.(string),
			EndKeyId:   endKeyID.(string),
			Requests:   uint64(requests.(int64)),
		}

		sample := samples[sampleID.(string)]
		sample.Buckets = append(sample.Buckets, bucket)
	}

	res := make([]*serverpb.KeyVisSamplesResponse_KeyVisSample, 0)

	for _, sample := range samples {
		res = append(res, sample)
	}

	return res, nil
}

func ReadKeys(
	ctx context.Context,
	ie *sql.InternalExecutor,
) (map[string]roachpb.Key, error) {

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
		keyUUID, err := datumToNative(row[0])
		if err != nil {
			return nil, err
		}
		keyBytes, err := datumToNative(row[1])
		if err != nil {
			return nil, err
		}

		keys[keyUUID.(string)] = keyBytes.([]byte)
	}

	return keys, nil

}
