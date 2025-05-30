// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func registerLargeKV(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "storage/large_kv",
		Owner:            registry.OwnerStorage,
		Cluster:          r.MakeClusterSpec(1),
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.Suites(registry.Roachtest),
		Timeout:          60 * time.Minute,
		Run:              runLargeKeyAndValueKV,
	})
}

func runLargeKeyAndValueKV(ctx context.Context, t test.Test, c cluster.Cluster) {
	const (
		dbNode     = 1
		tableName  = "largekv"
		keySize    = 1 << 20       // 1MB
		valueSize  = 1 << 20       // 1MB
		targetSize = 2<<30 + 1<<29 // Target total data in bytes (2.5GB)
	)

	numPairs := targetSize / (keySize + valueSize)
	t.L().Printf("Inserting %d key-value pairs of %d bytes each to exceed %.1fGB", numPairs, keySize+valueSize, targetSize/float64(1<<30))

	t.Status("starting cockroach cluster")
	settings := install.MakeClusterSettings()
	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), settings, c.All())

	conn := c.Conn(ctx, t.L(), dbNode)
	defer func() {
		if err := conn.Close(); err != nil {
			t.L().Printf("error closing connection: %v", err)
		}
	}()

	t.Status("creating table")
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (k BYTES PRIMARY KEY, v BYTES)`, tableName)); err != nil {
		t.Fatal(errors.Wrap(err, "failed to create table"))
	}

	// Prevent splits so all data stays in one range (and thus one Pebble block).
	zoneBytes := int64(targetSize * 1.1)
	rangeMinBytes := zoneBytes / 2
	rangeMaxBytes := zoneBytes
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s CONFIGURE ZONE USING num_replicas = 1, range_min_bytes = %d, range_max_bytes = %d`, tableName, rangeMinBytes, rangeMaxBytes)); err != nil {
		t.Fatal(errors.Wrap(err, "failed to configure zone to prevent splits"))
	}

	seed := timeutil.Now().UnixNano()
	t.L().Printf("PRNG seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))

	// Sample the first, middle, and last keys for verification.
	sampleKeys := map[int][]byte{
		0:            nil,
		numPairs / 2: nil,
		numPairs - 1: nil,
	}
	for i := 0; i < numPairs; i++ {
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		// Use a fixed prefix to keep all keys in the same range.
		copy(key, []byte(fmt.Sprintf("prefix-%08d-", i)))
		rng.Read(key[len("prefix-00000000-"):])
		rng.Read(value)
		label := fmt.Sprintf("kv_%d", i)
		t.Status(fmt.Sprintf("inserting %s", label))
		start := timeutil.Now()
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`UPSERT INTO %s (k, v) VALUES ($1, $2)`, tableName), key, value); err != nil {
			t.Fatal(errors.Wrapf(err, "[%s] insert failed", label))
		}
		t.L().Printf("[%s] inserted in %s", label, timeutil.Since(start))
		// Store sample keys for verification
		for s := range sampleKeys {
			if i == s {
				sampleKeys[s] = slices.Clone(key)
			}
		}
	}

	t.Status("verifying a sample of inserted keys")
	for i := range sampleKeys {
		key := sampleKeys[i]
		label := fmt.Sprintf("sample_kv_%d", i)
		// Read
		t.Status(fmt.Sprintf("[%s] reading", label))
		var got []byte
		start := timeutil.Now()
		if err := conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT v FROM %s WHERE k = $1`, tableName), key).Scan(&got); err != nil {
			t.Fatal(errors.Wrapf(err, "[%s] read failed", label))
		}
		t.L().Printf("[%s] read in %s", label, timeutil.Since(start))
		// Update
		t.Status(fmt.Sprintf("[%s] updating", label))
		newValue := make([]byte, valueSize)
		rng.Read(newValue)
		start = timeutil.Now()
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`UPDATE %s SET v = $1 WHERE k = $2`, tableName), newValue, key); err != nil {
			t.Fatal(errors.Wrapf(err, "[%s] update failed", label))
		}
		t.L().Printf("[%s] updated in %s", label, timeutil.Since(start))
		// Delete
		t.Status(fmt.Sprintf("[%s] deleting", label))
		start = timeutil.Now()
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE k = $1`, tableName), key); err != nil {
			t.Fatal(errors.Wrapf(err, "[%s] delete failed", label))
		}
		t.L().Printf("[%s] deleted in %s", label, timeutil.Since(start))
	}

	// Perform a scan query to verify range scan works.
	t.Status("verifying scan query over key range")
	firstKey := sampleKeys[0]
	lastKey := sampleKeys[numPairs-1]
	var scanCount int
	rows, err := conn.QueryContext(ctx, fmt.Sprintf(
		`SELECT length(v) FROM %s WHERE k >= $1 AND k <= $2`, tableName),
		firstKey, lastKey)
	if err != nil {
		t.Fatal(errors.Wrap(err, "scan query failed"))
	}
	defer rows.Close()
	for rows.Next() {
		var l int
		if err := rows.Scan(&l); err != nil {
			t.Fatal(errors.Wrap(err, "scan row failed"))
		}
		if l != valueSize {
			t.Fatalf("unexpected value length: got %d, want %d", l, valueSize)
		}
		scanCount++
	}
	t.L().Printf("scan query returned %d rows", scanCount)

	// one row for each key in the range, minus the sample keys as they are deleted above.
	expectedRows := numPairs - len(sampleKeys)
	if scanCount != expectedRows {
		t.Fatalf("scan query returned %d rows, expected %d", scanCount, expectedRows)
	}

	t.Status("large multi-KV block test completed successfully")
}
