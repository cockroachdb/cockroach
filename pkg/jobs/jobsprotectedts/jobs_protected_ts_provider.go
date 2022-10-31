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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// timedProtectTimeStampGCPct wait a percentage of the GC time before
// creating a protected timestamp record.
const timedProtectTimeStampGCPct = 80

type protectedTimestampForJobProvider struct {
	db                  *kv.DB
	codec               keys.SQLCodec
	protectedTSProvider protectedts.Provider
	systemConfig        config.SystemConfigProvider
	jr                  *jobs.Registry
}

type protectedTimestampForJobRecord struct {
	waitGrp                  ctxgroup.Group
	protectedTSInstallCancel chan struct{}
	unprotectCallback        func(ctx context.Context) error
	jobID                    int64
}

func setProtectedTSOnJob(details jobspb.Details, u *uuid.UUID) jobspb.Details {
	switch v := details.(type) {
	case jobspb.NewSchemaChangeDetails:
		v.ProtectedTimestampRecord = u
		return v
	case jobspb.SchemaChangeDetails:
		v.ProtectedTimestampRecord = u
		return v
	default:
		panic("not supported")
	}
}

func getProtectedTSOnJob(details jobspb.Details) *uuid.UUID {
	switch v := details.(type) {
	case jobspb.NewSchemaChangeDetails:
		return v.ProtectedTimestampRecord
	case jobspb.SchemaChangeDetails:
		return v.ProtectedTimestampRecord
	default:
		panic("not supported")
	}
}

// NewProtectedTimestampForJobProvider creates a new protected timestamp provider
// for a job.
func NewProtectedTimestampForJobProvider(
	db *kv.DB,
	codec keys.SQLCodec,
	protectedTSProvider protectedts.Provider,
	systemConfig config.SystemConfigProvider,
	jr *jobs.Registry,
) scexec.ProtectedTimestampForJobProvider {
	return &protectedTimestampForJobProvider{
		db:                  db,
		codec:               codec,
		protectedTSProvider: protectedTSProvider,
		systemConfig:        systemConfig,
		jr:                  jr,
	}
}

// Protect implements scexec.ProtectedTimestampForJobProvider.
func (p *protectedTimestampForJobProvider) Protect(
	ctx context.Context,
	jobID jobspb.JobID,
	tableDesc catalog.TableDescriptor,
	readAsOf hlc.Timestamp,
) scexec.ProtectedTimestampForJobCleaner {
	jr := &protectedTimestampForJobRecord{
		waitGrp:                  ctxgroup.WithContext(ctx),
		protectedTSInstallCancel: make(chan struct{}),
		jobID:                    int64(jobID),
	}
	jr.waitGrp.GoCtx(func(ctx context.Context) error {
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
		waitBeforeProtectedTS := ((time.Duration(zoneCfg.GC.TTLSeconds) * time.Second) *
			timedProtectTimeStampGCPct) / 100

		select {
		case <-time.After(waitBeforeProtectedTS):
			// If we are not running a historical query, nothing to do here.
			if readAsOf.IsEmpty() {
				return nil
			}

			var protectedtsID *uuid.UUID
			err := p.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				job, err := p.jr.LoadJobWithTxn(ctx, jobID, txn)
				if err != nil {
					return err
				}
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

						target := ptpb.MakeSchemaObjectsTarget(descpb.IDs{tableDesc.GetID()})
						rec := MakeRecord(*protectedtsID,
							jr.jobID, readAsOf, nil, Jobs, target)
						return p.protectedTSProvider.Protect(ctx, txn, rec)
					})
				}
				// Refresh the existing timestamp.
				return p.protectedTSProvider.UpdateTimestamp(ctx, txn, *protectedtsID, readAsOf)
			})
			if err != nil {
				return err
			}
			jr.unprotectCallback = func(ctx context.Context) error {
				return p.Unprotect(ctx, jobID)

			}
		case <-jr.protectedTSInstallCancel:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	})
	return jr
}

// Unprotect implements scexec.ProtectedTimestampForJobProvider.
func (p *protectedTimestampForJobProvider) Unprotect(
	ctx context.Context, jobID jobspb.JobID,
) error {
	return p.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		job, err := p.jr.LoadJobWithTxn(ctx, jobID, txn)
		if err != nil {
			return err
		}
		// Fetch the protected timestamp UUID from the job, if one exists.
		details := job.Details()
		protectedtsID := getProtectedTSOnJob(details)
		if protectedtsID == nil {
			return nil
		}
		// If we do find one then we need to clean up the protected timestamp,
		// and remove it from the job.
		return job.Update(ctx, txn, func(txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater) error {
			details = md.Payload.UnwrapDetails()
			details = setProtectedTSOnJob(details, nil)
			md.Payload.Details = jobspb.WrapPayloadDetails(details)
			ju.UpdatePayload(md.Payload)
			return p.protectedTSProvider.Release(ctx, txn, *protectedtsID)
		})
	})
}

// WaitAndMaybeUnprotect implements scexec.ProtectedTimestampForJobCleaner.
func (r *protectedTimestampForJobRecord) WaitAndMaybeUnprotect(ctx context.Context) error {
	close(r.protectedTSInstallCancel)
	if err := r.waitGrp.Wait(); err != nil {
		return err
	}
	if r.unprotectCallback != nil {
		if err := r.unprotectCallback(ctx); err != nil {
			return err
		}
	}
	return nil
}
