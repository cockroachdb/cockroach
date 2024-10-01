// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracedumper

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTraceDumperZipCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	traceDir := filepath.Join(baseDir, "trace_dir")
	require.NoError(t, os.Mkdir(traceDir, 0755))

	baseTime := time.Date(2019, time.January, 1, 0, 0, 0, 0, time.UTC)
	td := TraceDumper{
		currentTime: func() time.Time {
			return baseTime
		},
		store: dumpstore.NewStore(traceDir, nil, nil),
	}
	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	filename := "foo"
	td.Dump(ctx, filename, 123, s.InternalExecutor().(isql.Executor))
	expectedFilename := fmt.Sprintf("%s.%s.%s.zip", jobTraceDumpPrefix, baseTime.Format(timeFormat),
		filename)
	fullpath := td.store.GetFullPath(expectedFilename)
	_, err := os.Stat(fullpath)
	require.NoError(t, err)
}
