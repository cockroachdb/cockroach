// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingest

import (
	"context"
	"net/url"
	"os"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func streamIngestionJobDescription(
	p sql.PlanHookState, streamIngestion *tree.StreamIngestion,
) (string, error) {
	ann := p.ExtendedEvalContext().Annotations
	return tree.AsStringWithFQNames(streamIngestion, ann), nil
}

// maybeAddInlineSecurityCredentials converts a PG URL into sslinline mode by adding ssl key and
// certificates inline as ssl parameters. sslinline is postgres driver specific feature
// (https://github.com/lib/pq/blob/8446d16b8935fdf2b5c0fe333538ac395e3e1e4b/ssl.go#L85).
func maybeAddInlineSecurityCredentials(pgURL url.URL) (url.URL, error) {
	options := pgURL.Query()
	if options.Get("sslinline") == "true" {
		return pgURL, nil
	}

	loadPathContentAsOption := func(optionKey string) error {
		if !options.Has(optionKey) {
			return nil
		}
		content, err := os.ReadFile(options.Get(optionKey))
		if err != nil {
			return err
		}
		options.Set(optionKey, string(content))
		return nil
	}

	if err := loadPathContentAsOption("sslrootcert"); err != nil {
		return url.URL{}, err
	}
	// Convert client certs inline.
	if options.Get("sslmode") == "verify-full" {
		if err := loadPathContentAsOption("sslcert"); err != nil {
			return url.URL{}, err
		}
		if err := loadPathContentAsOption("sslkey"); err != nil {
			return url.URL{}, err
		}
	}
	options.Set("sslinline", "true")
	res := pgURL
	res.RawQuery = options.Encode()
	return res, nil
}

func ingestionPlanHook(
	ctx context.Context, stmt tree.Statement, p sql.PlanHookState,
) (sql.PlanHookRowFn, colinfo.ResultColumns, []sql.PlanNode, bool, error) {
	ingestionStmt, ok := stmt.(*tree.StreamIngestion)
	if !ok {
		return nil, nil, nil, false, nil
	}

	// Check if the experimental feature is enabled.
	if !p.SessionData().EnableStreamReplication {
		return nil, nil, nil, false, errors.WithTelemetry(
			pgerror.WithCandidateCode(
				errors.WithHint(
					errors.Newf("stream replication is only supported experimentally"),
					"You can enable stream replication by running `SET enable_experimental_stream_replication = true`.",
				),
				pgcode.FeatureNotSupported,
			),
			"replication.ingest.disabled",
		)
	}

	fromFn, err := p.TypeAsStringArray(ctx, tree.Exprs(ingestionStmt.From), "INGESTION")
	if err != nil {
		return nil, nil, nil, false, err
	}

	fn := func(ctx context.Context, _ []sql.PlanNode, resultsCh chan<- tree.Datums) error {
		ctx, span := tracing.ChildSpan(ctx, stmt.StatementTag())
		defer span.Finish()

		if err := utilccl.CheckEnterpriseEnabled(
			p.ExecCfg().Settings, p.ExecCfg().ClusterID(), p.ExecCfg().Organization(),
			"RESTORE FROM REPLICATION STREAM",
		); err != nil {
			return err
		}

		from, err := fromFn()
		if err != nil {
			return err
		}

		// We only support a TENANT target, so error out if that is nil.
		if !ingestionStmt.Targets.TenantID.IsSet() {
			return errors.Newf("no tenant specified in ingestion query: %s", ingestionStmt.String())
		}

		streamAddress := streamingccl.StreamAddress(from[0])
		url, err := streamAddress.URL()
		if err != nil {
			return err
		}
		q := url.Query()

		// Operator should specify a postgres scheme address with cert authentication.
		if hasPostgresAuthentication := (q.Get("sslmode") == "verify-full") &&
			q.Has("sslrootcert") && q.Has("sslkey") && q.Has("sslcert"); (url.Scheme == "postgres") && !hasPostgresAuthentication {
			return errors.Errorf(
				"stream replication address should have cert authentication if in postgres scheme: %s", streamAddress)
		}

		// Convert this URL into sslinline mode.
		*url, err = maybeAddInlineSecurityCredentials(*url)
		if err != nil {
			return err
		}
		streamAddress = streamingccl.StreamAddress(url.String())

		if ingestionStmt.Targets.Types != nil || ingestionStmt.Targets.Databases != nil ||
			ingestionStmt.Targets.Tables != nil || ingestionStmt.Targets.Schemas != nil {
			return errors.Newf("unsupported target in ingestion query, "+
				"only tenant ingestion is supported: %s", ingestionStmt.String())
		}

		// TODO(adityamaru): Add privileges checks. Probably the same as RESTORE.

		prefix := keys.MakeTenantPrefix(ingestionStmt.Targets.TenantID.TenantID)
		startTime := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		if ingestionStmt.AsOf.Expr != nil {
			asOf, err := p.EvalAsOfTimestamp(ctx, ingestionStmt.AsOf)
			if err != nil {
				return err
			}
			startTime = asOf.Timestamp
		}

		streamIngestionDetails := jobspb.StreamIngestionDetails{
			StreamAddress: string(streamAddress),
			TenantID:      ingestionStmt.Targets.TenantID.TenantID,
			Span:          roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()},
			StartTime:     startTime,
		}

		jobDescription, err := streamIngestionJobDescription(p, ingestionStmt)
		if err != nil {
			return err
		}

		jr := jobs.Record{
			Description:   jobDescription,
			Username:      p.User(),
			Progress:      jobspb.StreamIngestionProgress{},
			Details:       streamIngestionDetails,
			NonCancelable: true,
		}

		jobID := p.ExecCfg().JobRegistry.MakeJobID()
		sj, err := p.ExecCfg().JobRegistry.CreateAdoptableJobWithTxn(ctx, jr,
			jobID, p.ExtendedEvalContext().Txn)
		if err != nil {
			return err
		}
		resultsCh <- tree.Datums{tree.NewDInt(tree.DInt(sj.ID()))}
		return nil
	}

	return fn, jobs.DetachedJobExecutionResultHeader, nil, false, nil
}

func init() {
	sql.AddPlanHook("ingestion", ingestionPlanHook)
	jobs.RegisterConstructor(
		jobspb.TypeStreamIngestion,
		func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &streamIngestionResumer{
				job: job,
			}
		},
	)
}
