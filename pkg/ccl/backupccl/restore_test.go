// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backupccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// A special jobs.Resumer that simulates interrupted resume by
// aborting resume after restore descriptors are published, and then
// resuming execution again.
var _ jobs.Resumer = &restartAfterPublishDescriptorsResumer{}

type restartAfterPublishDescriptorsResumer struct {
	t       *testing.T
	wrapped *restoreResumer
}

func (r *restartAfterPublishDescriptorsResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	e := errors.New("bail out")
	r.wrapped.testingKnobs.afterPublishingDescriptors = func() error {
		return e
	}
	require.Equal(r.t, e, r.wrapped.Resume(ctx, phs, resultsCh))
	r.wrapped.testingKnobs.afterPublishingDescriptors = nil
	return r.wrapped.Resume(ctx, phs, resultsCh)
}

func (r *restartAfterPublishDescriptorsResumer) OnFailOrCancel(
	ctx context.Context, phs interface{},
) error {
	return nil
}

func TestRestorePrivilegesChanged(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir
	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(ctx)

	tc.Server(0).JobRegistry().(*jobs.Registry).TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
		// Arrange for our special job resumer to be returned.
		jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
			return &restartAfterPublishDescriptorsResumer{
				t:       t,
				wrapped: raw.(*restoreResumer),
			}
		},
	}

	runner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
	runner.Exec(t, `
CREATE USER user_to_drop;
CREATE TABLE foo (k INT PRIMARY KEY, v BYTES);
GRANT SELECT ON TABLE foo TO user_to_drop;
BACKUP TABLE foo TO 'nodelocal://0/foo';
DROP TABLE foo;
DROP ROLE user_to_drop;
RESTORE TABLE foo FROM 'nodelocal://0/foo';
`)
}
