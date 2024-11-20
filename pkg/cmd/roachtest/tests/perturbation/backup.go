// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package perturbation

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// backup will run a backup during the perturbation phase.
type backup struct{}

var _ perturbation = backup{}

func (b backup) setup() variations {
	return setup(b, 5.0)
}

// TODO(baptist): Add variation for incremental backup.
// TODO(baptist): Add variation with revision history.
func (b backup) setupMetamorphic(rng *rand.Rand) variations {
	return b.setup().randomize(rng)
}

func (backup) startTargetNode(ctx context.Context, t test.Test, v variations) {
	v.startNoBackup(ctx, t, v.targetNodes())
}

func (backup) startPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	startTime := timeutil.Now()
	db := v.Conn(ctx, t.L(), v.targetNodes()[0])
	defer db.Close()
	var bucketPrefix string
	backupTestingBucket := testutils.BackupTestingBucket()
	switch v.Cloud() {
	case spec.GCE:
		bucketPrefix = "gs"
	case spec.AWS:
		bucketPrefix = "s3"
	case spec.Azure:
		bucketPrefix = "azure"
	default:
		bucketPrefix = "nodelocal"
		backupTestingBucket = strconv.Itoa(v.targetNodes()[0])
	}
	backupURL := fmt.Sprintf("%s://%s/perturbation-backups/%s?AUTH=implicit", bucketPrefix, backupTestingBucket, v.Name())
	cmd := fmt.Sprintf(`BACKUP INTO '%s' AS OF SYSTEM TIME '-10s'`, backupURL)
	_, err := db.ExecContext(ctx, cmd)
	require.NoError(t, err)
	return timeutil.Since(startTime)
}

func (backup) endPerturbation(ctx context.Context, t test.Test, v variations) time.Duration {
	waitDuration(ctx, v.validationDuration)
	return v.validationDuration
}
