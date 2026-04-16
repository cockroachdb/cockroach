// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package screl

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/walkutil"
)

// WalkDescIDs calls f for every catid.DescID field in e.
func WalkDescIDs(e scpb.Element, f func(id *catid.DescID) error) error {
	return walkutil.Walk(reflect.TypeOf((*catid.DescID)(nil)), e, func(i interface{}) error {
		return f(i.(*catid.DescID))
	})
}

// WalkTypes calls f for every *types.T field in e.
func WalkTypes(e scpb.Element, f func(t *types.T) error) error {
	return walkutil.Walk(reflect.TypeOf((*types.T)(nil)), e, func(i interface{}) error {
		return f(i.(*types.T))
	})
}

// WalkExpressions calls f for every catpb.Expression field in e.
func WalkExpressions(e scpb.Element, f func(t *catpb.Expression) error) error {
	return walkutil.Walk(reflect.TypeOf((*catpb.Expression)(nil)), e, func(i interface{}) error {
		return f(i.(*catpb.Expression))
	})
}

// WalkColumnIDs calls f for every catid.ColumnID field in e.
func WalkColumnIDs(e scpb.Element, f func(id *catid.ColumnID) error) error {
	return walkutil.Walk(reflect.TypeOf((*catid.ColumnID)(nil)), e, func(i interface{}) error {
		return f(i.(*catid.ColumnID))
	})
}

func WalkConstraintIDs(e scpb.Element, f func(id *catid.ConstraintID) error) error {
	return walkutil.Walk(reflect.TypeOf((*catid.ConstraintID)(nil)), e, func(i interface{}) error {
		return f(i.(*catid.ConstraintID))
	})
}

// WalkTriggerIDs calls f for every catid.TriggerID field in e.
func WalkTriggerIDs(e scpb.Element, f func(id *catid.TriggerID) error) error {
	return walkutil.Walk(reflect.TypeOf((*catid.TriggerID)(nil)), e, func(i interface{}) error {
		return f(i.(*catid.TriggerID))
	})
}

func WalkIndexIDs(e scpb.Element, f func(id *catid.IndexID) error) error {
	return walkutil.Walk(reflect.TypeOf((*catid.IndexID)(nil)), e, func(i interface{}) error {
		return f(i.(*catid.IndexID))
	})
}
