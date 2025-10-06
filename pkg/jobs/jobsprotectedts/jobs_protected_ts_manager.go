// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobsprotectedts

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// timedProtectTimeStampGCPct wait a percentage of the GC TTL before
// creating a protected timestamp record.
const timedProtectTimeStampGCPct = 0.8

// Manager manages protected timestamps installed for jobs, which will
// install protected timestamps after a certain percentage of the GC interval
// is hit.
type Manager struct {
	db                  isql.DB
	codec               keys.SQLCodec
	protectedTSProvider protectedts.Manager
	systemConfig        config.SystemConfigProvider
	jr                  *jobs.Registry
}

// Cleaner cleans up the protected timestamp record for the job or cancels
// the installation.
type Cleaner func(ctx context.Context) error

func setProtectedTSOnJob(details jobspb.Details, u *uuid.UUID) jobspb.Details {
	switch v := details.(type) {
	case jobspb.RestoreDetails:
		v.ProtectedTimestampRecord = u
		return v
	case jobspb.NewSchemaChangeDetails:
		v.ProtectedTimestampRecord = u
		return v
	case jobspb.SchemaChangeDetails:
		v.ProtectedTimestampRecord = u
		return v
	default:
		panic(errors.AssertionFailedf("not supported %T", details))
	}
}

func getProtectedTSOnJob(details jobspb.Details) *uuid.UUID {
	switch v := details.(type) {
	case jobspb.RestoreDetails:
		return v.ProtectedTimestampRecord
	case jobspb.NewSchemaChangeDetails:
		return v.ProtectedTimestampRecord
	case jobspb.SchemaChangeDetails:
		return v.ProtectedTimestampRecord
	default:
		panic("not supported")
	}
}

// NewManager creates a new protected timestamp manager
// for jobs.
func NewManager(
	db isql.DB,
	codec keys.SQLCodec,
	protectedTSProvider protectedts.Manager,
	systemConfig config.SystemConfigProvider,
	jr *jobs.Registry,
) *Manager {
	return &Manager{
		db:                  db,
		codec:               codec,
		protectedTSProvider: protectedTSProvider,
		systemConfig:        systemConfig,
		jr:                  jr,
	}
}

// TryToProtectBeforeGC adds a protected timestamp record for a historical
// transaction for a specific table, once a certain percentage of the GC TTL has
// elapsed. This method can be preferred in scenarios where the cost of
// installing a protected timestamp is more expensive relative to the typical
// length of an operation (for example multi region). The approach here is
// heuristic and can be considered a best-effort basis since the GC TTL could
// change or the caller may not invoke this early enough in the transaction.
// Returns a Cleaner function to cancel installation or remove the protected
// timestamp. Note, the function assumes the in-memory job is up to date with
// the persisted job record.
func (p *Manager) TryToProtectBeforeGC(
	ctx context.Context, job *jobs.Job, tableDesc catalog.TableDescriptor, readAsOf hlc.Timestamp,
) Cleaner {
	waitGrp := ctxgroup.WithContext(ctx)
	protectedTSInstallCancel := make(chan struct{})
	var unprotectCallback Cleaner
	waitGrp.GoCtx(func(ctx context.Context) error {
		// If we are starting up, the system config can be nil. We are okay letting
		// the job restart due to the GC interval and lack of protected timestamp.
		systemConfig := p.systemConfig.GetSystemConfig()
		if systemConfig == nil {
			return nil
		}
		// Determine what the GC interval is on the table, which will help us
		// figure out when to apply a protected timestamp as a percentage of the
		// time until GC can occur.
		zoneCfg, err := systemConfig.GetZoneConfigForObject(p.codec,
			config.ObjectID(tableDesc.GetID()))
		if err != nil {
			return err
		}
		waitBeforeProtectedTS := time.Duration(0)
		now := timeutil.Now()
		readAsOfTime := timeutil.Unix(0, readAsOf.WallTime)
		gcTTL := time.Duration(zoneCfg.GC.TTLSeconds) * time.Second
		if readAsOfTime.Add(gcTTL).After(now) {
			timeUntilGC := readAsOfTime.Add(gcTTL).Sub(now)
			waitBeforeProtectedTS = time.Duration(float64(timeUntilGC) * timedProtectTimeStampGCPct)
		}

		select {
		case <-time.After(waitBeforeProtectedTS):
			target := ptpb.MakeSchemaObjectsTarget(descpb.IDs{tableDesc.GetID()})
			unprotectCallback, err = p.Protect(ctx, job, target, readAsOf)
			if err != nil {
				return err
			}
		case <-protectedTSInstallCancel:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	return func(ctx context.Context) error {
		close(protectedTSInstallCancel)
		if err := waitGrp.Wait(); err != nil {
			return err
		}
		if unprotectCallback != nil {
			if err := unprotectCallback(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

// Protect adds a protected timestamp record for a historical transaction for a
// specific table immediately in a synchronous manner. If an existing record is
// found, it will be updated with a new timestamp. Returns a Cleaner function to
// remove the protected timestamp, if one was installed. Note, the function
// assumes the in-memory job is up to date with the persisted job record.
func (p *Manager) Protect(
	ctx context.Context, job *jobs.Job, target *ptpb.Target, readAsOf hlc.Timestamp,
) (Cleaner, error) {
	// If we are not running a historical query, nothing to do here.
	if readAsOf.IsEmpty() {
		return nil, nil
	}
	// Set up a new protected timestamp ID and install it on the job.
	err := job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		// Check if the protected timestamp is visible in the txn.
		protectedtsID := getProtectedTSOnJob(md.Payload.UnwrapDetails())
		// If it's been removed lets create a new one.
		pts := p.protectedTSProvider.WithTxn(txn)
		if protectedtsID == nil {
			newID := uuid.MakeV4()
			protectedtsID = &newID
			details := setProtectedTSOnJob(md.Payload.UnwrapDetails(), protectedtsID)
			md.Payload.Details = jobspb.WrapPayloadDetails(details)
			ju.UpdatePayload(md.Payload)
			rec := MakeRecord(*protectedtsID,
				int64(job.ID()), readAsOf, nil, Jobs, target)
			return pts.Protect(ctx, rec)
		}
		// Refresh the existing timestamp, otherwise.
		return pts.UpdateTimestamp(ctx, *protectedtsID, readAsOf)
	})
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		// Remove the protected timestamp.
		return p.Unprotect(ctx, job)
	}, nil
}

// Unprotect the pts associated with the job, mainly for last resort cleanup.
// The function assumes the in-memory job is up to date with the persisted job
// record. Note: This should only be used for job cleanup if is not currently,
// executing.
func (p *Manager) Unprotect(ctx context.Context, job *jobs.Job) error {
	// Fetch the protected timestamp UUID from the job, if one exists.
	if getProtectedTSOnJob(job.Details()) == nil {
		return nil
	}
	// If we do find one then we need to clean up the protected timestamp,
	// and remove it from the job.
	return job.NoTxn().Update(ctx, func(txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		// The job will get refreshed, so check one more time the protected
		// timestamp still exists. The callback returned from Protect works
		// on a previously cached copy.
		protectedtsID := getProtectedTSOnJob(md.Payload.UnwrapDetails())
		if protectedtsID == nil {
			return nil
		}
		updatedDetails := setProtectedTSOnJob(md.Payload.UnwrapDetails(), nil)
		md.Payload.Details = jobspb.WrapPayloadDetails(updatedDetails)
		ju.UpdatePayload(md.Payload)
		return p.protectedTSProvider.WithTxn(txn).Release(ctx, *protectedtsID)
	})
}
