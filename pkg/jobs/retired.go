// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

func init() {
	// The following are types of job that have been retired and are replaced by a
	// no-op executor that will allow any existing jobs to be moved to a terminal
	// state and thus eventually be cleaned up.
	for _, typ := range []jobspb.Type{
		jobspb.TypeAutoConfigRunner,
		jobspb.TypeAutoConfigEnvRunner,
		jobspb.TypeAutoConfigTask,
	} {
		RegisterConstructor(typ, noopConstructor, DisablesTenantCostControl)
	}
}

func noopConstructor(_ *Job, _ *cluster.Settings) Resumer {
	return noopResumer{}
}

type noopResumer struct{}

func (r noopResumer) Resume(ctx context.Context, _ interface{}) error {
	return nil
}
func (r noopResumer) OnFailOrCancel(_ context.Context, _ interface{}, _ error) error {
	return nil
}
func (r noopResumer) CollectProfile(_ context.Context, _ interface{}) error {
	return nil
}
