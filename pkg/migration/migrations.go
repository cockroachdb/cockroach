// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import (
	"context"
	"time"

	cv "github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Registry defines the global mapping between a version, and the associated
// migration. The migration is only executed after a cluster-wide bump of the
// version gate.
var Registry = make(map[roachpb.Version]Migration)

func init() {
	Registry[cv.VersionByKey(cv.VersionNoopMigration)] = NoopMigration
}

// NoopMigration is, well, a no-op migration. All it does is sleep for while
// before returning.
func NoopMigration(ctx context.Context, h *Helper) error {
	time.Sleep(time.Second)
	log.Info(ctx, "ran no-op migration")
	return nil
}
