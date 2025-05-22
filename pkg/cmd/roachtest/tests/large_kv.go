// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"crypto/rand"
	"fmt"
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
		CompatibleClouds: registry.OnlyLocal,
		Suites:           registry.Suites(registry.Roachtest),
		Timeout:          60 * time.Minute,
		Run:              runLargeKeyAndValueKV,
	})
}

func runLargeKeyAndValueKV(ctx context.Context, t test.Test, c cluster.Cluster) {
	const (
		dbNode    = 1
		tableName = "largekv"
		keySize   = 1 << 20 // 1MB
		valueSize = 1 << 20 // 1MB
		targetGB  = 2.5     // Target total data in GB
	)

	numPairs := int((targetGB * (1 << 30)) / float64(keySize+valueSize))
	t.L().Printf("Inserting %d key-value pairs of %d bytes each to exceed %.1fGB", numPairs, keySize+valueSize, targetGB)

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
	zoneBytes := int64(targetGB * 1.1 * float64(1<<30))
	rangeMinBytes := zoneBytes / 2
	rangeMaxBytes := zoneBytes
	if _, err := conn.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s CONFIGURE ZONE USING num_replicas = 1, range_min_bytes = %d, range_max_bytes = %d`, tableName, rangeMinBytes, rangeMaxBytes)); err != nil {
		t.Fatal(errors.Wrap(err, "failed to configure zone to prevent splits"))
	}

	// Sample the first, middle, and last keys for verification.
	sample := []int{0, numPairs / 2, numPairs - 1}
	sampleKeys := make(map[int][]byte)
	for i := 0; i < numPairs; i++ {
		key := make([]byte, keySize)
		value := make([]byte, valueSize)
		// Use a fixed prefix to keep all keys in the same range.
		copy(key, []byte(fmt.Sprintf("prefix-%08d-", i)))
		if _, err := rand.Read(key[len("prefix-00000000-"):]); err != nil {
			t.Fatal(errors.Wrap(err, "failed to generate random key"))
		}
		if _, err := rand.Read(value); err != nil {
			t.Fatal(errors.Wrap(err, "failed to generate random value"))
		}
		label := fmt.Sprintf("kv_%d", i)
		t.Status(fmt.Sprintf("inserting %s", label))
		start := timeutil.Now()
		if _, err := conn.ExecContext(ctx, fmt.Sprintf(`UPSERT INTO %s (k, v) VALUES ($1, $2)`, tableName), key, value); err != nil {
			t.Fatal(errors.Wrapf(err, "[%s] insert failed", label))
		}
		t.L().Printf("[%s] inserted in %s", label, timeutil.Since(start))
		// Store sample keys for verification
		for _, s := range sample {
			if i == s {
				copied := make([]byte, len(key))
				copy(copied, key)
				sampleKeys[s] = copied
			}
		}
	}

	t.Status("verifying a sample of inserted keys")
	for _, i := range sample {
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
		if _, err := rand.Read(newValue); err != nil {
			t.Fatal(errors.Wrapf(err, "[%s] failed to generate random update value", label))
		}
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

	t.Status("large multi-KV block test completed successfully")
}
