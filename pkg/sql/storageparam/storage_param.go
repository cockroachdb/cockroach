// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package storageparam defines interfaces and functions for setting and
// resetting storage parameters.
package storageparam

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Setter applies a storage parameter to an underlying item.
type Setter interface {
	// Set is called during CREATE [TABLE | INDEX] ... WITH (...) or
	// ALTER [TABLE | INDEX] ... WITH (...).
	Set(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error
	// Reset is called during ALTER [TABLE | INDEX] ... RESET (...)
	Reset(ctx context.Context, evalCtx *eval.Context, key string) error
	// RunPostChecks is called after all storage parameters have been set.
	// This allows checking whether multiple storage parameters together
	// form a valid configuration.
	RunPostChecks() error
}

// Set sets the given storage parameters using the
// given observer.
func Set(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	params tree.StorageParams,
	setter Setter,
) error {
	for _, sp := range params {
		key := string(sp.Key)
		if sp.Value == nil {
			return pgerror.Newf(pgcode.InvalidParameterValue, "storage parameter %q requires a value", key)
		}
		telemetry.Inc(sqltelemetry.SetTableStorageParameter(key))

		// Expressions may be an unresolved name.
		// Cast these as strings.
		expr := paramparse.UnresolvedNameToStrVal(sp.Value)

		// Convert the expressions to a datum.
		typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, types.Any)
		if err != nil {
			return err
		}
		if typedExpr, err = normalize.Expr(ctx, evalCtx, typedExpr); err != nil {
			return err
		}
		datum, err := eval.Expr(ctx, evalCtx, typedExpr)
		if err != nil {
			return err
		}

		if err := setter.Set(ctx, semaCtx, evalCtx, key, datum); err != nil {
			return err
		}
	}
	return setter.RunPostChecks()
}

// Reset sets the given storage parameters using the
// given observer.
func Reset(
	ctx context.Context, evalCtx *eval.Context, params tree.NameList, paramObserver Setter,
) error {
	for _, p := range params {
		telemetry.Inc(sqltelemetry.ResetTableStorageParameter(string(p)))
		if err := paramObserver.Reset(ctx, evalCtx, string(p)); err != nil {
			return err
		}
	}
	return paramObserver.RunPostChecks()
}

// SetFillFactor validates the fill_factor storage param and then issues a
// notice.
func SetFillFactor(ctx context.Context, evalCtx *eval.Context, key string, datum tree.Datum) error {
	val, err := paramparse.DatumAsFloat(ctx, evalCtx, key, datum)
	if err != nil {
		return err
	}
	if val < 0 || val > 100 {
		return pgerror.Newf(pgcode.InvalidParameterValue, "%q must be between 0 and 100", key)
	}
	if evalCtx != nil {
		evalCtx.ClientNoticeSender.BufferClientNotice(
			ctx,
			pgnotice.Newf("storage parameter %q is ignored", key),
		)
	}
	return nil
}
