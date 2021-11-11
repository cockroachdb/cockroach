// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scsqldeps

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
)

// ValidateForwardIndexesFn callback function for validating forward indexes.
type ValidateForwardIndexesFn func(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	runHistoricalTxn sqlutil.HistoricalInternalExecTxnRunner,
	withFirstMutationPublic bool,
	gatherAllInvalid bool,
	execOverride sessiondata.InternalExecutorOverride,
) error

// ValidateInvertedIndexesFn callback function for validating inverted indexes.
type ValidateInvertedIndexesFn func(
	ctx context.Context,
	codec keys.SQLCodec,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	runHistoricalTxn sqlutil.HistoricalInternalExecTxnRunner,
	gatherAllInvalid bool,
	execOverride sessiondata.InternalExecutorOverride,
) error

// NewFakeSessionDataFn callback function used to create session data
// for the internal executor.
type NewFakeSessionDataFn func(sv *settings.Values) *sessiondata.SessionData

type indexValidator struct {
	db                      *kv.DB
	codec                   keys.SQLCodec
	settings                *cluster.Settings
	ieFactory               sqlutil.SessionBoundInternalExecutorFactory
	validateForwardIndexes  ValidateForwardIndexesFn
	validateInvertedIndexes ValidateInvertedIndexesFn
	newFakeSessionData      NewFakeSessionDataFn
}

// ValidateForwardIndexes checks that the indexes have entries for all the rows.
func (iv indexValidator) ValidateForwardIndexes(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	withFirstMutationPublic bool,
	gatherAllInvalid bool,
	override sessiondata.InternalExecutorOverride,
) error {
	// Set up a new transaction with the current timestamp.
	txnRunner := func(ctx context.Context, fn sqlutil.InternalExecFn) error {
		validationTxn := iv.db.NewTxn(ctx, "validation")
		err := validationTxn.SetFixedTimestamp(ctx, iv.db.Clock().Now())
		if err != nil {
			return err
		}
		return fn(ctx, validationTxn, iv.ieFactory(ctx, iv.newFakeSessionData(&iv.settings.SV)))
	}
	return iv.validateForwardIndexes(ctx, tableDesc, indexes, txnRunner, withFirstMutationPublic, gatherAllInvalid, override)
}

// ValidateInvertedIndexes checks that the indexes have entries for all the rows.
func (iv indexValidator) ValidateInvertedIndexes(
	ctx context.Context,
	tableDesc catalog.TableDescriptor,
	indexes []catalog.Index,
	gatherAllInvalid bool,
	override sessiondata.InternalExecutorOverride,
) error {
	// Set up a new transaction with the current timestamp.
	txnRunner := func(ctx context.Context, fn sqlutil.InternalExecFn) error {
		validationTxn := iv.db.NewTxn(ctx, "validation")
		err := validationTxn.SetFixedTimestamp(ctx, iv.db.Clock().Now())
		if err != nil {
			return err
		}
		return fn(ctx, validationTxn, iv.ieFactory(ctx, iv.newFakeSessionData(&iv.settings.SV)))
	}
	return iv.validateInvertedIndexes(ctx, iv.codec, tableDesc, indexes, txnRunner, gatherAllInvalid, override)
}

// NewIndexValidator creates a IndexValidator interface
// for the new schema changer.
func NewIndexValidator(
	db *kv.DB,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	ieFactory sqlutil.SessionBoundInternalExecutorFactory,
	validateForwardIndexes ValidateForwardIndexesFn,
	validateInvertedIndexes ValidateInvertedIndexesFn,
	newFakeSessionData NewFakeSessionDataFn,
) scexec.IndexValidator {
	return indexValidator{
		db:                      db,
		codec:                   codec,
		settings:                settings,
		ieFactory:               ieFactory,
		validateForwardIndexes:  validateForwardIndexes,
		validateInvertedIndexes: validateInvertedIndexes,
		newFakeSessionData:      newFakeSessionData,
	}
}
