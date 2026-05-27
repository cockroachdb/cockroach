// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/stretchr/testify/require"
)

// TestCreateTableAlwaysRoutesToLegacy verifies that createTableChecks rejects
// every CREATE TABLE shape so the statement falls back to the legacy schema
// changer.
func TestCreateTableAlwaysRoutesToLegacy(t *testing.T) {
	modes := []sessiondatapb.NewSchemaChangerMode{
		sessiondatapb.UseNewSchemaChangerOff,
		sessiondatapb.UseNewSchemaChangerOn,
		sessiondatapb.UseNewSchemaChangerUnsafe,
		sessiondatapb.UseNewSchemaChangerUnsafeAlways,
	}

	tests := []struct {
		name string
		stmt *tree.CreateTable
	}{
		{name: "empty CREATE TABLE", stmt: &tree.CreateTable{}},
		{name: "CREATE TABLE IF NOT EXISTS", stmt: &tree.CreateTable{IfNotExists: true}},
		{name: "CREATE TEMPORARY TABLE", stmt: &tree.CreateTable{Persistence: tree.PersistenceTemporary}},
		{name: "CREATE UNLOGGED TABLE", stmt: &tree.CreateTable{Persistence: tree.PersistenceUnlogged}},
		{name: "CREATE TABLE AS", stmt: &tree.CreateTable{AsSource: &tree.Select{}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range modes {
				require.False(t,
					IsFullySupportedWithFalsePositive(tc.stmt, clusterversion.ClusterVersion{}, mode),
					"mode=%v: CREATE TABLE must route to legacy", mode)
			}
		})
	}
}

// TestCreateTableBodyPanicsNotImplemented pins the defense-in-depth behavior:
// if a statement ever slips past createTableChecks, the builder body raises a
// NotImplementedError so the dispatcher falls back to the legacy planner
// instead of emitting a half-built descriptor.
func TestCreateTableBodyPanicsNotImplemented(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "CreateTable body must panic")
		err, ok := r.(error)
		require.True(t, ok, "panic value must be an error, got %T", r)
		require.True(t, scerrors.HasNotImplemented(err),
			"panic must be a NotImplementedError, got %+v", err)
	}()
	CreateTable(nil /* b */, &tree.CreateTable{})
}
