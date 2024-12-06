// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/redact"
)

// ValidateForwardIndexesFn callback function for validating forward indexes.
type ValidateForwardIndexesFn func(
	ctx context.Context,
	job *jobs.Job,
	tbl catalog.TableDescriptor,
	indexes []catalog.Index,
	runHistoricalTxn descs.HistoricalInternalExecTxnRunner,
	withFirstMutationPublic bool,
	gatherAllInvalid bool,
	execOverride sessiondata.InternalExecutorOverride,
	protectedTSProvider scexec.ProtectedTimestampManager,
) error

// ValidateInvertedIndexesFn callback function for validating inverted indexes.
type ValidateInvertedIndexesFn func(
	ctx context.Context,
	codec keys.SQLCodec,
	job *jobs.Job,
	tbl catalog.TableDescriptor,
	indexes []catalog.Index,
	runHistoricalTxn descs.HistoricalInternalExecTxnRunner,
	withFirstMutationPublic bool,
	gatherAllInvalid bool,
	execOverride sessiondata.InternalExecutorOverride,
	protectedTSProvider scexec.ProtectedTimestampManager,
) error

// ValidateConstraintFn callback function for validating constraints.
type ValidateConstraintFn func(
	ctx context.Context,
	tbl catalog.TableDescriptor,
	constraint catalog.Constraint,
	indexIDForValidation descpb.IndexID,
	sessionData *sessiondata.SessionData,
	runHistoricalTxn descs.HistoricalInternalExecTxnRunner,
	execOverride sessiondata.InternalExecutorOverride,
) error

// NewFakeSessionDataFn callback function used to create session data
// for the internal executor.
type NewFakeSessionDataFn func(ctx context.Context, settings *cluster.Settings, opName redact.SafeString) *sessiondata.SessionData

type validator struct {
	db                         *kv.DB
	codec                      keys.SQLCodec
	settings                   *cluster.Settings
	ieFactory                  isql.DB
	validateForwardIndexes     ValidateForwardIndexesFn
	validateInvertedIndexes    ValidateInvertedIndexesFn
	validateConstraint         ValidateConstraintFn
	newFakeSessionData         NewFakeSessionDataFn
	protectedTimestampProvider scexec.ProtectedTimestampManager
}

// ValidateForwardIndexes checks that the indexes have entries for all the rows.
func (vd validator) ValidateForwardIndexes(
	ctx context.Context,
	job *jobs.Job,
	tbl catalog.TableDescriptor,
	indexes []catalog.Index,
	override sessiondata.InternalExecutorOverride,
) error {
	const withFirstMutationPublic = true
	const gatherAllInvalid = false
	return vd.validateForwardIndexes(
		ctx, job, tbl, indexes, vd.makeHistoricalInternalExecTxnRunner(),
		withFirstMutationPublic, gatherAllInvalid, override, vd.protectedTimestampProvider,
	)
}

// ValidateInvertedIndexes checks that the indexes have entries for all the rows.
func (vd validator) ValidateInvertedIndexes(
	ctx context.Context,
	job *jobs.Job,
	tbl catalog.TableDescriptor,
	indexes []catalog.Index,
	override sessiondata.InternalExecutorOverride,
) error {

	const withFirstMutationPublic = true
	const gatherAllInvalid = false
	return vd.validateInvertedIndexes(
		ctx, vd.codec, job, tbl, indexes, vd.makeHistoricalInternalExecTxnRunner(),
		withFirstMutationPublic, gatherAllInvalid, override, vd.protectedTimestampProvider,
	)
}

func (vd validator) ValidateConstraint(
	ctx context.Context,
	tbl catalog.TableDescriptor,
	constraint catalog.Constraint,
	indexIDForValidation descpb.IndexID,
	override sessiondata.InternalExecutorOverride,
) error {
	return vd.validateConstraint(ctx, tbl, constraint, indexIDForValidation, vd.newFakeSessionData(ctx, vd.settings, "validate-constraint"),
		vd.makeHistoricalInternalExecTxnRunner(), override)
}

// makeHistoricalInternalExecTxnRunner creates a new transaction runner which
// always runs at the same time and that time is the current time as of when
// this constructor was called.
func (vd validator) makeHistoricalInternalExecTxnRunner() descs.HistoricalInternalExecTxnRunner {
	now := vd.db.Clock().Now()
	return descs.NewHistoricalInternalExecTxnRunner(now, func(ctx context.Context, fn descs.InternalExecFn) error {
		return vd.ieFactory.(descs.DB).DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			if err := txn.KV().SetFixedTimestamp(ctx, now); err != nil {
				return err
			}
			return fn(ctx, txn)
		}, isql.WithPriority(admissionpb.BulkNormalPri))
	})
}

// NewValidator creates a Validator interface
// for the new schema changer.
func NewValidator(
	db *kv.DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	ieFactory isql.DB,
	protectedTimestampProvider scexec.ProtectedTimestampManager,
	validateForwardIndexes ValidateForwardIndexesFn,
	validateInvertedIndexes ValidateInvertedIndexesFn,
	validateCheckConstraint ValidateConstraintFn,
	newFakeSessionData NewFakeSessionDataFn,
) scexec.Validator {
	return validator{
		db:                         db,
		codec:                      codec,
		settings:                   settings,
		ieFactory:                  ieFactory,
		validateForwardIndexes:     validateForwardIndexes,
		validateInvertedIndexes:    validateInvertedIndexes,
		validateConstraint:         validateCheckConstraint,
		newFakeSessionData:         newFakeSessionData,
		protectedTimestampProvider: protectedTimestampProvider,
	}
}
