// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationutils

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// GetAlterConnectionChecker returns a function that will poll for an altered
// external connection. The initial URI passed to this function should be the
// same one passed to subsequent client creation calls.
func GetAlterConnectionChecker(
	id jobspb.JobID,
	initialURI string,
	uriGetter URIGetter,
	execCfg *sql.ExecutorConfig,
	stopper chan struct{},
) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		pollingInterval := 2 * time.Minute
		if knobs := execCfg.StreamingTestingKnobs; knobs != nil && knobs.ExternalConnectionPollingInterval != nil {
			pollingInterval = *knobs.ExternalConnectionPollingInterval
		}
		t := time.NewTicker(pollingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-stopper:
				return nil
			case <-t.C:
				reloadedJob, err := execCfg.JobRegistry.LoadJob(ctx, id)
				if err != nil {
					return err
				}
				newURI, err := resolveURI(ctx, execCfg, uriGetter(reloadedJob.Details()))
				if err != nil {
					log.Dev.Warningf(ctx, "failed to load uri: %v", err)
				} else if newURI != initialURI {
					return errors.Mark(errors.Newf("uri has been updated: old=%s, new=%s", errors.Redact(initialURI), errors.Redact(newURI)), sql.ErrPlanChanged)
				}
			}
		}
	}
}

type URIGetter func(details jobspb.Details) string

func resolveURI(
	ctx context.Context, execCfg *sql.ExecutorConfig, sourceURI string,
) (string, error) {
	configUri, err := streamclient.ParseConfigUri(sourceURI)
	if err != nil {
		return "", err
	}

	clusterUri, err := configUri.AsClusterUri(ctx, execCfg.InternalDB)
	if err != nil {
		return "", err
	}

	return clusterUri.Serialize(), nil
}
