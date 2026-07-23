// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keyvisstorage

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const timeFormatString = "2006-01-02 15:04:05"

func writeSample(
	ctx context.Context, ie *sql.InternalExecutor, sampleTime time.Time,
) (string, error) {

	timeString := sampleTime.Format(timeFormatString)

	stmt := fmt.Sprintf("INSERT INTO system.span_stats_samples ("+
		"sample_time) VALUES (TIMESTAMP '%s') RETURNING id", timeString)

	row, err := ie.QueryRowEx(
		ctx,
		"write-sample",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		stmt,
	)

	if err != nil {
		return "", err
	}
	id := tree.MustBeDUuid(row[0]).UUID.String()
	return id, nil
}

func writeNewKeys(ctx context.Context, ie *sql.InternalExecutor, newKeys map[string]string) error {
	values := make([]string, 0)
	for hexEncoded, keyUUID := range newKeys {
		value := fmt.Sprintf("('%s', x'%s')", keyUUID, hexEncoded)
		values = append(values, value)
	}

	if len(values) == 0 {
		return nil
	}

	stmt := fmt.Sprintf("INSERT INTO system.span_stats_unique_keys (id, "+
		"key_bytes) VALUES %s", strings.Join(values, ","))

	_, err := ie.ExecEx(ctx, "write-new-keys", nil,
		sessiondata.NodeUserSessionDataOverride, stmt)

	return err
}

func writeBuckets(
	ctx context.Context,
	ie *sql.InternalExecutor,
	sampleID string,
	existingKeys map[string]string,
	newKeys map[string]string,
	sample keyvispb.Sample,
) error {

	values := make([]string, 0)

	for _, stat := range sample.SpanStats {
		startKeyHex := hex.EncodeToString(stat.Span.Key)
		endKeyHex := hex.EncodeToString(stat.Span.EndKey)

		var startKeyID string
		var endKeyID string

		if keyUUID, ok := existingKeys[startKeyHex]; ok {
			startKeyID = keyUUID
		} else {
			startKeyID = newKeys[startKeyHex]
		}

		if keyUUID, ok := existingKeys[endKeyHex]; ok {
			endKeyID = keyUUID
		} else {
			endKeyID = newKeys[endKeyHex]
		}

		rowValue := fmt.Sprintf("('%s', '%s', '%s', %d)", sampleID, startKeyID,
			endKeyID, stat.Requests)

		values = append(values, rowValue)
	}

	if len(values) == 0 {
		return nil
	}

	stmt := fmt.Sprintf("INSERT INTO system.span_stats_buckets ("+
		"sample_id, start_key_id, end_key_id, requests) VALUES %s",
		strings.Join(values, ","))

	_, err := ie.ExecEx(
		ctx,
		"write-buckets",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		stmt,
	)

	return err
}

func getKeys(
	ctx context.Context, ie *sql.InternalExecutor, sample keyvispb.Sample,
) (map[string]string, map[string]string, error) {

	// query unique_keys for all keys in the sample
	keysAsString := make([]string, 0)
	for _, bucket := range sample.SpanStats {
		startKeyAsString := hex.EncodeToString(bucket.Span.Key)
		endKeyAsString := hex.EncodeToString(bucket.Span.EndKey)
		keysAsString = append(keysAsString, "X'"+startKeyAsString+"'")
		keysAsString = append(keysAsString, "X'"+endKeyAsString+"'")
	}

	stmt := fmt.Sprintf("SELECT * FROM system.span_stats_unique_keys "+
		"WHERE key_bytes IN (%s)", strings.Join(keysAsString, ","))

	rows, err := ie.QueryBufferedEx(
		ctx,
		"query-unique-keys",
		nil,
		sessiondata.NodeUserSessionDataOverride,
		stmt,
	)

	if err != nil {
		return nil, nil, err
	}

	// need a map of bytes -> uuid.
	existingKeys := make(map[string]string)
	for _, row := range rows {
		id := tree.MustBeDUuid(row[0]).UUID.String()
		bytes := tree.MustBeDBytes(row[1])
		existingKeys[hex.EncodeToString(roachpb.Key(bytes))] = id
	}

	newKeysToWrite := make(map[string]string)
	for _, bucket := range sample.SpanStats {

		// is the start key in existing keys?
		start := hex.EncodeToString(bucket.Span.Key)
		end := hex.EncodeToString(bucket.Span.EndKey)

		if _, ok := existingKeys[start]; !ok {
			newKeysToWrite[start] = uuid.MakeV4().String()
		}

		if _, ok := existingKeys[end]; !ok {
			newKeysToWrite[end] = uuid.MakeV4().String()
		}
	}

	return existingKeys, newKeysToWrite, nil
}

// WriteSamples persists the keyvispb.GetSamplesResponse.
func WriteSamples(ctx context.Context, ie *sql.InternalExecutor, samples []keyvispb.Sample) error {
	for _, sample := range samples {

		// write a new sample, returning the primary key.
		sampleID, err := writeSample(ctx, ie, sample.SampleTime)
		if err != nil {
			return err
		}

		// of all keys in this sample, which are new, and which exist?
		existingKeys, newKeysToWrite, err := getKeys(ctx, ie, sample)
		if err != nil {
			return err
		}

		// write new keys
		if err := writeNewKeys(ctx, ie, newKeysToWrite); err != nil {
			return err
		}

		// write new buckets
		if err := writeBuckets(
			ctx, ie, sampleID, existingKeys, newKeysToWrite, sample,
		); err != nil {
			return err
		}
	}

	return nil
}
