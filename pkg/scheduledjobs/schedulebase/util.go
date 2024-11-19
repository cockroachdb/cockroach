// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schedulebase

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
	cron "github.com/robfig/cron/v3"
)

// ScheduleRecurrence is a helper struct used to set the schedule recurrence.
type ScheduleRecurrence struct {
	// User specified cron expression indicating how often the schdule should
	// be run
	Cron string

	// The "evaluated" form of the Cron string, in Go native data type
	Frequency time.Duration
}

// NeverRecurs is a sentinel value indicating the schedule never recurs.
var NeverRecurs *ScheduleRecurrence

func frequencyFromCron(now time.Time, cronStr string) (time.Duration, error) {
	expr, err := cron.ParseStandard(cronStr)
	if err != nil {
		return 0, pgerror.Newf(pgcode.InvalidParameterValue,
			`error parsing schedule expression: %q; it must be a valid cron expression`,
			cronStr)
	}
	nextRun := expr.Next(now)
	return expr.Next(nextRun).Sub(nextRun), nil
}

// ComputeScheduleRecurrence creates ScheduleRecurrence struct from crontab
// notation
func ComputeScheduleRecurrence(now time.Time, rec *string) (*ScheduleRecurrence, error) {
	if rec == nil {
		return NeverRecurs, nil
	}
	cronStr := *rec
	frequency, err := frequencyFromCron(now, cronStr)
	if err != nil {
		return nil, err
	}

	return &ScheduleRecurrence{cronStr, frequency}, nil
}

// CheckScheduleAlreadyExists returns true if a schedule with the same label
// already exists.
func CheckScheduleAlreadyExists(
	ctx context.Context, p sql.PlanHookState, scheduleLabel string,
) (bool, error) {
	row, err := p.InternalSQLTxn().QueryRowEx(ctx, "check-sched",
		p.Txn(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("SELECT count(schedule_name) FROM %s WHERE schedule_name = '%s'",
			scheduledjobs.ProdJobSchedulerEnv.ScheduledJobsTableName(), scheduleLabel))

	if err != nil {
		return false, err
	}
	return int64(tree.MustBeDInt(row[0])) != 0, nil
}

// ParseOnError parses schedule option optOnExecFailure into
// jobspb.ScheduleDetails
func ParseOnError(onError string, details *jobspb.ScheduleDetails) error {
	switch strings.ToLower(onError) {
	case "retry":
		details.OnError = jobspb.ScheduleDetails_RETRY_SOON
	case "reschedule":
		details.OnError = jobspb.ScheduleDetails_RETRY_SCHED
	case "pause":
		details.OnError = jobspb.ScheduleDetails_PAUSE_SCHED
	default:
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"%q is not a valid on_execution_error; valid values are [retry|reschedule|pause]",
			onError)
	}
	return nil
}

// ParseWaitBehavior parses schedule option optOnPreviousRunning into
// jobspb.ScheduleDetails
func ParseWaitBehavior(wait string, details *jobspb.ScheduleDetails) error {
	switch strings.ToLower(wait) {
	case "start":
		details.Wait = jobspb.ScheduleDetails_NO_WAIT
	case "skip":
		details.Wait = jobspb.ScheduleDetails_SKIP
	case "wait":
		details.Wait = jobspb.ScheduleDetails_WAIT
	default:
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"%q is not a valid on_previous_running; valid values are [start|skip|wait]",
			wait)
	}
	return nil
}

// ParseOnPreviousRunningOption parses optOnPreviousRunning from
// jobspb.ScheduleDetails_WaitBehavior
func ParseOnPreviousRunningOption(
	onPreviousRunning jobspb.ScheduleDetails_WaitBehavior,
) (string, error) {
	var onPreviousRunningOption string
	switch onPreviousRunning {
	case jobspb.ScheduleDetails_WAIT:
		onPreviousRunningOption = "WAIT"
	case jobspb.ScheduleDetails_NO_WAIT:
		onPreviousRunningOption = "START"
	case jobspb.ScheduleDetails_SKIP:
		onPreviousRunningOption = "SKIP"
	default:
		return onPreviousRunningOption, pgerror.Newf(pgcode.InvalidParameterValue,
			"%s is an invalid onPreviousRunning option",
			onPreviousRunning.String())
	}
	return onPreviousRunningOption, nil
}

// ParseOnErrorOption parses optOnExecFailure from
// jobspb.ScheduleDetails_ErrorHandlingBehavior
func ParseOnErrorOption(onError jobspb.ScheduleDetails_ErrorHandlingBehavior) (string, error) {
	var onErrorOption string
	switch onError {
	case jobspb.ScheduleDetails_RETRY_SCHED:
		onErrorOption = "RESCHEDULE"
	case jobspb.ScheduleDetails_RETRY_SOON:
		onErrorOption = "RETRY"
	case jobspb.ScheduleDetails_PAUSE_SCHED:
		onErrorOption = "PAUSE"
	default:
		return onErrorOption, pgerror.Newf(pgcode.InvalidParameterValue,
			"%s is an invalid onError option",
			onError.String())
	}
	return onErrorOption, nil
}

// FullyQualifyTables fully qualifies the table names by resolving the table,
// database and schema.
func FullyQualifyTables(
	ctx context.Context, p sql.PlanHookState, tables tree.TablePatterns,
) ([]tree.TablePattern, error) {
	fqTablePatterns := make([]tree.TablePattern, len(tables))
	for i, target := range tables {
		tablePattern, err := target.NormalizeTablePattern()
		if err != nil {
			return nil, err
		}
		switch tp := tablePattern.(type) {
		case *tree.TableName:
			if err := sql.DescsTxn(ctx, p.ExecCfg(), func(
				ctx context.Context, txn isql.Txn, col *descs.Collection,
			) error {
				// Resolve the table.
				un := tp.ToUnresolvedObjectName()
				found, _, tableDesc, err := resolver.ResolveExisting(
					ctx, un, p, tree.ObjectLookupFlags{DesiredObjectKind: tree.TableObject},
					p.CurrentDatabase(), p.CurrentSearchPath(),
				)
				if err != nil {
					return err
				}
				if !found {
					return errors.Newf("target table %s could not be resolved", tp.String())
				}

				// Resolve the database.
				dbDesc, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, tableDesc.GetParentID())
				if err != nil {
					return err
				}

				// Resolve the schema.
				schemaDesc, err := col.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Schema(ctx, tableDesc.GetParentSchemaID())
				if err != nil {
					return err
				}
				tn := tree.NewTableNameWithSchema(
					tree.Name(dbDesc.GetName()),
					tree.Name(schemaDesc.GetName()),
					tree.Name(tableDesc.GetName()),
				)
				fqTablePatterns[i] = tn
				return nil
			}); err != nil {
				return nil, err
			}
		case *tree.AllTablesSelector:
			if !tp.ExplicitSchema {
				tp.ExplicitSchema = true
				tp.SchemaName = tree.Name(p.CurrentDatabase())
			} else if tp.ExplicitSchema && !tp.ExplicitCatalog {
				// The schema field could either be a schema or a database. If we can
				// successfully resolve the schema, we will add the DATABASE prefix.
				// Otherwise, no updates are needed since the schema field refers to the
				// database.
				var schemaID descpb.ID
				if err := sql.DescsTxn(ctx, p.ExecCfg(), func(
					ctx context.Context, txn isql.Txn, col *descs.Collection,
				) error {
					dbDesc, err := col.ByNameWithLeased(txn.KV()).Get().Database(ctx, p.CurrentDatabase())
					if err != nil {
						return err
					}
					schemaID, err = col.LookupSchemaID(ctx, txn.KV(), dbDesc.GetID(), tp.SchemaName.String())
					return err
				}); err != nil {
					return nil, err
				}

				if schemaID != descpb.InvalidID {
					tp.ExplicitCatalog = true
					tp.CatalogName = tree.Name(p.CurrentDatabase())
				}
			}
			fqTablePatterns[i] = tp
		}
	}
	return fqTablePatterns, nil
}
