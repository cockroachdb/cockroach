// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scdeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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

// ValidateCheckConstraintFn callback function for validting check constraints.
type ValidateCheckConstraintFn func(
	ctx context.Context,
	tbl catalog.TableDescriptor,
	constraint catalog.Constraint,
	sessionData *sessiondata.SessionData,
	runHistoricalTxn descs.HistoricalInternalExecTxnRunner,
	execOverride sessiondata.InternalExecutorOverride,
) error

// NewFakeSessionDataFn callback function used to create session data
// for the internal executor.
type NewFakeSessionDataFn func(sv *settings.Values) *sessiondata.SessionData

type validator struct {
	db                         *kv.DB
	codec                      keys.SQLCodec
	settings                   *cluster.Settings
	ieFactory                  sqlutil.InternalExecutorFactory
	validateForwardIndexes     ValidateForwardIndexesFn
	validateInvertedIndexes    ValidateInvertedIndexesFn
	validateCheckConstraint    ValidateCheckConstraintFn
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

func (vd validator) ValidateCheckConstraint(
	ctx context.Context,
	tbl catalog.TableDescriptor,
	constraint catalog.Constraint,
	override sessiondata.InternalExecutorOverride,
) error {
	return vd.validateCheckConstraint(ctx, tbl, constraint, vd.newFakeSessionData(&vd.settings.SV),
		vd.makeHistoricalInternalExecTxnRunner(), override)
}

// makeHistoricalInternalExecTxnRunner creates a new transaction runner which
// always runs at the same time and that time is the current time as of when
// this constructor was called.
func (vd validator) makeHistoricalInternalExecTxnRunner() descs.HistoricalInternalExecTxnRunner {
	now := vd.db.Clock().Now()
	return descs.NewHistoricalInternalExecTxnRunner(now, func(ctx context.Context, fn descs.InternalExecFn) error {
		return vd.ieFactory.(descs.TxnManager).DescsTxnWithExecutor(ctx, vd.db, vd.newFakeSessionData(&vd.settings.SV), func(
			ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, ie sqlutil.InternalExecutor,
		) error {
			if err := txn.SetFixedTimestamp(ctx, now); err != nil {
				return err
			}
			return fn(ctx, txn, ie, descriptors)
		})
	})
}

// NewValidator creates a Validator interface
// for the new schema changer.
func NewValidator(
	db *kv.DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	ieFactory sqlutil.InternalExecutorFactory,
	protectedTimestampProvider scexec.ProtectedTimestampManager,
	validateForwardIndexes ValidateForwardIndexesFn,
	validateInvertedIndexes ValidateInvertedIndexesFn,
	validateCheckConstraint ValidateCheckConstraintFn,
	newFakeSessionData NewFakeSessionDataFn,
) scexec.Validator {
	return validator{
		db:                         db,
		codec:                      codec,
		settings:                   settings,
		ieFactory:                  ieFactory,
		validateForwardIndexes:     validateForwardIndexes,
		validateInvertedIndexes:    validateInvertedIndexes,
		validateCheckConstraint:    validateCheckConstraint,
		newFakeSessionData:         newFakeSessionData,
		protectedTimestampProvider: protectedTimestampProvider,
	}
}
