// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sctest

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TestServerFactory constructs test clusters for declarative schema changer
// end-to-end tests.
type TestServerFactory interface {

	// WithSchemaChangerKnobs returns a copy of this factory but which
	// will install the provided declarative schema changer testing knobs.
	WithSchemaChangerKnobs(knobs *scexec.TestingKnobs) TestServerFactory

	// WithMixedVersion returns a copy of this factory but which will
	// create a cluster in a mixed-version state. The active version
	// will be the minimum supported binary version.
	WithMixedVersion() TestServerFactory

	// Run creates a test cluster and applies fn to it.
	Run(
		ctx context.Context,
		t *testing.T,
		fn func(s serverutils.TestServerInterface, tdb *gosql.DB),
	)
}

// SingleNodeTestClusterFactory is the vanilla implementation of
// the TestServerFactory interface.
type SingleNodeTestClusterFactory struct {
	server *server.TestingKnobs
	scexec *scexec.TestingKnobs
}

var _ TestServerFactory = SingleNodeTestClusterFactory{}

// WithSchemaChangerKnobs implements the TestServerFactory interface.
func (f SingleNodeTestClusterFactory) WithSchemaChangerKnobs(
	knobs *scexec.TestingKnobs,
) TestServerFactory {
	f.scexec = knobs
	return f
}

// WithMixedVersion implements the TestServerFactory interface.
func (f SingleNodeTestClusterFactory) WithMixedVersion() TestServerFactory {
	f.server = &server.TestingKnobs{
		ClusterVersionOverride:         OldVersionKey.Version(),
		DisableAutomaticVersionUpgrade: make(chan struct{}),
	}
	return f
}

// Run implements the TestServerFactory interface.
func (f SingleNodeTestClusterFactory) Run(
	ctx context.Context, t *testing.T, fn func(_ serverutils.TestServerInterface, _ *gosql.DB),
) {
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: newJobsKnobs(),
			SQLExecutor: &sql.ExecutorTestingKnobs{
				UseTransactionalDescIDGenerator: true,
			},
		},
		// TODO(postamar): remove this
		DefaultTestTenant: base.TODOTestTenantDisabled,
	}
	if f.server != nil {
		args.Knobs.Server = f.server
	}
	if f.scexec != nil {
		args.Knobs.SQLDeclarativeSchemaChanger = f.scexec
	}
	s, db, _ := serverutils.StartServer(t, args)
	defer func() {
		s.Stopper().Stop(ctx)
	}()
	fn(s, db)
}

// OldVersionKey is the version key used by the WithMixedVersion method
// in the TestServerFactory interface.
const OldVersionKey = clusterversion.MinSupported

// newJobsKnobs constructs jobs.TestingKnobs for the end-to-end tests.
func newJobsKnobs() *jobs.TestingKnobs {
	jobKnobs := jobs.NewTestingKnobsWithShortIntervals()

	// We want to force the process of marking the job as successful
	// to fail sometimes. This will ensure that the schema change job
	// is idempotent.
	var injectedFailures = struct {
		syncutil.Mutex
		m map[jobspb.JobID]struct{}
	}{
		m: make(map[jobspb.JobID]struct{}),
	}
	jobKnobs.BeforeUpdate = func(orig, updated jobs.JobMetadata) error {
		sc := orig.Payload.GetNewSchemaChange()
		if sc == nil {
			return nil
		}
		if orig.State != jobs.StateRunning || updated.State != jobs.StateSucceeded {
			return nil
		}
		injectedFailures.Lock()
		defer injectedFailures.Unlock()
		if _, ok := injectedFailures.m[orig.ID]; !ok {
			injectedFailures.m[orig.ID] = struct{}{}
			log.Infof(context.Background(), "injecting failure while marking job succeeded")
			return errors.New("injected failure when marking succeeded")
		}
		return nil
	}
	return jobKnobs
}
