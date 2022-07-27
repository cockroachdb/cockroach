package keyvisstorage

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanstats/spanstatspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"strings"
)

const timeFormatString = "2006-01-02 15:04:05"


func writeSample(
	ctx context.Context,
	ie *sql.InternalExecutor,
	sampleTime *hlc.Timestamp,
) (string, error) {

	timeString := sampleTime.GoTime().Format(timeFormatString)

	stmt := fmt.Sprintf("INSERT INTO system.span_stats_samples ("+
		"sample_time) VALUES (TIMESTAMP '%s') RETURNING id", timeString)

	row, err := ie.QueryRowEx(
		ctx,
		"write-sample",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		stmt,
	)

	if err != nil {
		return "", err
	}

	id, err := datumToNative(row[0])

	if err != nil {
		return "", err
	}

	return id.(string), nil
}

func writeNewKeys(
	ctx context.Context,
	ie *sql.InternalExecutor,
	newKeys map[string]string,
) error {
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


	log.Infof(ctx, "newkeys: %s", stmt)

	_, err := ie.ExecEx(ctx, "write-new-keys", nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()}, stmt)


	return err
}

func writeBuckets(
	ctx context.Context,
	ie *sql.InternalExecutor,
	sampleID string,
	existingKeys map[string]string,
	newKeys map[string]string,
	sample *spanstatspb.Sample) error {

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
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		stmt,
	)

	return err
}

func getKeys(
	ctx context.Context,
	ie *sql.InternalExecutor,
	sample *spanstatspb.Sample,
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
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		stmt,
	)

	if err != nil {
		return nil, nil, err
	}

	// need a map of bytes -> uuid.
	existingKeys := make(map[string]string)
	for _, row := range rows {
		u, err := datumToNative(row[0])
		if err != nil {
			return nil, nil, err
		}
		b, err := datumToNative(row[1])
		if err != nil {
			return nil, nil, err
		}
		bytes := b.([]byte)
		existingKeys[hex.EncodeToString(bytes)] = u.(string)
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

func WriteSamples(
	ctx context.Context,
	ie *sql.InternalExecutor,
	samples *keyvispb.GetSamplesResponse,
) error {
	for _, sample := range samples.Samples {

		// write a new sample, returning the primary key.
		sampleID, err := writeSample(ctx, ie, sample.SampleTime)
		if err != nil {
			return err
		}

		// of all keys in this sample, which are new, and which exist?
		existingKeys, newKeysToWrite, err := getKeys(ctx, ie, sample)

		for hexEncoded, keyId := range newKeysToWrite {
			log.Infof(ctx, "writenewkey: %s - %s", hexEncoded, keyId)
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
