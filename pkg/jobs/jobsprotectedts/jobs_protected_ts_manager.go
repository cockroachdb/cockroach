// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobsprotectedts

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	db                  *kv.DB
	codec               keys.SQLCodec
	protectedTSProvider protectedts.Provider
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
	db *kv.DB,
	codec keys.SQLCodec,
	protectedTSProvider protectedts.Provider,
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
		// If we are starting up the system config can be nil, we are okay letting
		// the job restart, due to the GC interval and lack of protected timestamp.
		systemConfig := p.systemConfig.GetSystemConfig()
		if systemConfig == nil {
			return nil
		}
		// Determine what the GC interval is on the table, which will help us
		// figure out when to apply a protected timestamp, as a percentage of this
		// time.
		zoneCfg, err := systemConfig.GetZoneConfigForObject(p.codec,
			config.ObjectID(tableDesc.GetID()))
		if err != nil {
			return err
		}
		waitBeforeProtectedTS := time.Duration((time.Duration(zoneCfg.GC.TTLSeconds) * time.Second).Seconds() *
			timedProtectTimeStampGCPct)

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
	var protectedtsID *uuid.UUID
	err := p.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		details := job.Details()
		protectedtsID = getProtectedTSOnJob(details)
		// Check if there is an existing protected timestamp ID on the job,
		// in which case we can only need to update it.
		if protectedtsID == nil {
			newID := uuid.MakeV4()
			protectedtsID = &newID
			// Set up a new protected timestamp ID and install it on the job.
			return job.Update(ctx, txn, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
				details = job.Details()
				details = setProtectedTSOnJob(details, protectedtsID)
				md.Payload.Details = jobspb.WrapPayloadDetails(details)
				ju.UpdatePayload(md.Payload)

				rec := MakeRecord(*protectedtsID,
					int64(job.ID()), readAsOf, nil, Jobs, target)
				return p.protectedTSProvider.Protect(ctx, txn, rec)
			})
		}
		// Refresh the existing timestamp.
		return p.protectedTSProvider.UpdateTimestamp(ctx, txn, *protectedtsID, readAsOf)
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
	return job.Update(ctx, nil, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
		// The job will get refreshed, so check one more time the protected
		// timestamp still exists. The callback returned from Protect works
		// on a previously cached copy.
		protectedtsID := getProtectedTSOnJob(md.Payload.UnwrapDetails())
		if protectedtsID == nil {
			return nil
		}
		updatedDetails := setProtectedTSOnJob(job.Details(), nil)
		md.Payload.Details = jobspb.WrapPayloadDetails(updatedDetails)
		ju.UpdatePayload(md.Payload)
		return p.protectedTSProvider.Release(ctx, txn, *protectedtsID)
	})
}
