// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package evalexpr contains tools for type checking and evaluating expressions.
package evalexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// ToTypeCheck is a set of expressions for type checking. The implementations
// hold onto the expected type for their respective members.
type ToTypeCheck interface {
	typeCheck(ctx context.Context, op string, semaCtx *tree.SemaContext) error
}

// TypeCheck will perform type checking on the passed exprs.
func TypeCheck(
	ctx context.Context, op string, semaCtx *tree.SemaContext, checkables ...ToTypeCheck,
) error {
	for _, c := range checkables {
		if err := c.typeCheck(ctx, op, semaCtx); err != nil {
			return err
		}
	}
	return nil
}

// Implementations to ToTypeCheck.
type (
	// StringArrays contains []Exprs which should all type check to containing
	// only strings.
	StringArrays []tree.Exprs
	// Strings contains Exprs which should all type check to string.
	Strings []tree.Expr
	// Ints contains Exprs which should all type check to int.
	Ints []tree.Expr
	// Bools contains Exprs which should all type check to bool.
	Bools []tree.Expr

	// KVOptions contains key-value pairs which should type check to
	// string and should conform to the validation policy described
	// in Validation.
	KVOptions struct {
		tree.KVOptions
		Validation KVOptionValidationMap
	}
)

var (
	_ ToTypeCheck = (StringArrays)(nil)
	_ ToTypeCheck = (Strings)(nil)
	_ ToTypeCheck = (Ints)(nil)
	_ ToTypeCheck = (Bools)(nil)
	_ ToTypeCheck = (*KVOptions)(nil)
)

// MakeStringArraysFromOptList makes a StringArrays by casting the members of in.
func MakeStringArraysFromOptList(in []tree.StringOrPlaceholderOptList) StringArrays {
	ret := make([]tree.Exprs, len(in))
	for i, exprs := range in {
		ret[i] = tree.Exprs(exprs)
	}
	return ret
}

func (i Ints) typeCheck(ctx context.Context, op string, semaCtx *tree.SemaContext) error {
	for _, expr := range i {
		if expr == nil {
			continue
		}
		if _, err := tree.TypeCheckAndRequire(
			ctx, expr, semaCtx, types.Int, op,
		); err != nil {
			return err
		}
	}
	return nil
}

func (k KVOptions) typeCheck(ctx context.Context, op string, semaCtx *tree.SemaContext) error {
	for _, opt := range k.KVOptions {
		if err := k.Validation.validate(opt); err != nil {
			return err
		}
		if opt.Value == nil {
			continue
		}
		if _, err := tree.TypeCheckAndRequire(
			ctx, opt.Value, semaCtx, types.String, op,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s Strings) typeCheck(ctx context.Context, op string, semaCtx *tree.SemaContext) error {
	for _, expr := range s {
		if expr == nil {
			continue
		}
		if _, err := tree.TypeCheckAndRequire(
			ctx, expr, semaCtx, types.String, op,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s StringArrays) typeCheck(ctx context.Context, op string, semaCtx *tree.SemaContext) error {
	for _, exprs := range s {
		if err := Strings(exprs).typeCheck(ctx, op, semaCtx); err != nil {
			return err
		}
	}
	return nil
}

func (b Bools) typeCheck(ctx context.Context, op string, semaCtx *tree.SemaContext) error {
	for _, expr := range b {
		if expr == nil {
			continue
		}
		if _, err := tree.TypeCheckAndRequire(
			ctx, expr, semaCtx, types.Bool, op,
		); err != nil {
			return err
		}
	}
	return nil
}
