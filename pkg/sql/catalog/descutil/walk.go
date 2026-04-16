// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descutil

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/walkutil"
)

// WalkDescIDs calls f for every catid.DescID field in the table descriptor.
func WalkDescIDs(desc *descpb.TableDescriptor, f func(id *catid.DescID) error) error {
	return walkutil.Walk(reflect.TypeOf((*catid.DescID)(nil)), desc, func(v interface{}) error {
		return f(v.(*catid.DescID))
	})
}

// WalkTypes calls f for every *types.T field in the table descriptor.
func WalkTypes(desc *descpb.TableDescriptor, f func(t *types.T) error) error {
	return walkutil.Walk(reflect.TypeOf((*types.T)(nil)), desc, func(v interface{}) error {
		return f(v.(*types.T))
	})
}

// WalkExpressions calls f for every catpb.Expression field in the table descriptor.
func WalkExpressions(desc *descpb.TableDescriptor, f func(expr *catpb.Expression) error) error {
	return walkutil.Walk(reflect.TypeOf((*catpb.Expression)(nil)), desc, func(v interface{}) error {
		return f(v.(*catpb.Expression))
	})
}

// WalkStatements calls f for every catpb.Statement field in the table descriptor.
func WalkStatements(desc *descpb.TableDescriptor, f func(stmt *catpb.Statement) error) error {
	return walkutil.Walk(reflect.TypeOf((*catpb.Statement)(nil)), desc, func(v interface{}) error {
		return f(v.(*catpb.Statement))
	})
}

// WalkRoutineBodies calls f for every catpb.RoutineBody field in the table descriptor.
func WalkRoutineBodies(desc *descpb.TableDescriptor, f func(body *catpb.RoutineBody) error) error {
	return walkutil.Walk(reflect.TypeOf((*catpb.RoutineBody)(nil)), desc, func(v interface{}) error {
		return f(v.(*catpb.RoutineBody))
	})
}
