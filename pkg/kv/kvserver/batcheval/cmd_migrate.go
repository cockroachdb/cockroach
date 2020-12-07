// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.Migrate, declareKeysMigrate, Migrate)
}

func declareKeysMigrate(
	_ *roachpb.RangeDescriptor, _ roachpb.Header, _ roachpb.Request, _, _ *spanset.SpanSet,
) {
}

// migrationRegistry is a global registry of all KV-level migrations. See
// pkg/migration for details around how the migrations defined here are
// wired up.
var migrationRegistry = make(map[roachpb.Version]migration)

type migration func(context.Context, storage.ReadWriter, CommandArgs) (result.Result, error)

func init() {
	// registerMigration(clusterversion.WhateverMigration, whateverMigration)
	_ = registerMigration
}

func registerMigration(key clusterversion.Key, migration migration) {
	migrationRegistry[clusterversion.ByKey(key)] = migration
}

// Migrate executes the below-raft migration corresponding to the given version.
func Migrate(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.MigrateRequest)

	fn, ok := migrationRegistry[args.Version]
	if !ok {
		return result.Result{}, errors.Newf("migration for %s not found", args.Version)
	}
	return fn(ctx, readWriter, cArgs)
}
