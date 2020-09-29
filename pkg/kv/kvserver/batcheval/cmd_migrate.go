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
	"github.com/cockroachdb/cockroach/pkg/util/log"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(roachpb.Migrate, declareKeysMigrate, Migrate)
}

func declareKeysMigrate(
	_ *roachpb.RangeDescriptor, _ roachpb.Header, _ roachpb.Request, _, _ *spanset.SpanSet,
) {
}

// Migrate ensures that the range proactively carries out any outstanding
// below-Raft migrations.
func Migrate(
	ctx context.Context, _ storage.ReadWriter, cArgs CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.MigrateRequest)
	h := cArgs.Header

	log.Infof(ctx, "xxx: evaluating Migrate command for r%d [%s, %s)", h.RangeID, args.Key, args.EndKey)

	return result.Result{}, nil
}
