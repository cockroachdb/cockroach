// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type intents struct{}

// TODO(baptist): Add a variation for the intents being cleaned up by MVCC GC
// instead of a select transaction.
// TODO(baptist): Add a variation where the intents are resolved by a low
// priority transaction.
var _ perturbation = intents{}

const batchSize = 100

func (i intents) setup() variations {
	// Most of the slowdown happens during intent resolution.
	return setup(i, 40.0)
}

func (i intents) setupMetamorphic(rng *rand.Rand) variations {
	v := i.setup()
	v = v.randomize(rng)

	// TODO(#139187): Large block sizes result in a big slowdown when the intents
	// are written.
	// TODO(#139188): Large block sizes result in a big slowdown when the intents
	// are resolved.
	v.blockSize = min(v.blockSize, 1024)

	// TODO(#135934): A large buildup of intents still causes a large slowdown
	// when the intents are resolved.
	v.perturbationDuration = max(v.perturbationDuration, 10*time.Minute)
	return v
}

func (intents) startTargetNode(ctx context.Context, t test.Test, v variations) {
	v.startNoBackup(ctx, t, v.targetNodes())
}

// TODO(baptist): This doesn't work with non-KV workloads. Handle that when we
// add new workloads. Originally this put the rows into another database, but
// that doesn't cause LSM inversion when they are cleaned up. They need to be
// interspersed with non-intent rows.
func (intents) startPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	startTime := timeutil.Now()
	db := v.Conn(ctx, t.L(), v.targetNodes()[0])
	defer db.Close()
	// Minimize foreground work by running transactions at low priority.
	_, err := db.ExecContext(ctx, "SET default_transaction_quality_of_service = 'background'")
	require.NoError(t, err)

	// Fills the table with a lot of intents and then rollback during the
	// perturbation duration. Each iteration creates `batchSize` intents. The
	// inserts are pretty fast so it will create many intents. The size of
	// each intent is based on the test's block size.
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	rng, _ := randutil.NewPseudoRand()
	i := 0
	for ; timeutil.Since(startTime) < v.perturbationDuration; i++ {
		startKey := int64(rng.Uint64())
		bytes := make([]byte, v.blockSize)
		for b := range bytes {
			bytes[b] = byte(rng.Int() & 0xff)
		}
		// NB: This uses upsert to avoid conflicts with existing rows.
		query := fmt.Sprintf(`UPSERT INTO target.kv(k, v) SELECT %d + i, $1 FROM `+
			`generate_series(1, %d) AS t(i);`, startKey, batchSize)
		_, err := tx.ExecContext(ctx, query, bytes)
		require.NoError(t, err)
	}
	t.L().Printf("created %d intents", batchSize*i)
	require.NoError(t, tx.Rollback())
	return timeutil.Since(startTime)
}

// endPerturbation will scan the table to force resolving all the intents.
func (intents) endPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	startTime := timeutil.Now()
	db := v.Conn(ctx, t.L(), v.targetNodes()[0])
	defer db.Close()
	// Scan the table to force resolving all the intents. Use read committed to
	// avoid causing conflicts with normal KV writes.
	_, err := db.ExecContext(ctx, "SET default_transaction_isolation = 'read committed'; SELECT count(*) FROM target.kv")
	require.NoError(t, err)
	t.L().Printf("intents resolved")
	// Wait a little longer to make sure the system fully recovers.
	waitDuration(ctx, v.validationDuration/2)
	return timeutil.Since(startTime)
}
