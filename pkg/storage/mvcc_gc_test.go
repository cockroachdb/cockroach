// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

/*



 */
func TestMVCCGarbageCollect2(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	datadriven.Walk(t, testutils.TestDataPath(t, "mvcc_garbage_collect"), func(t *testing.T, path string) {
		engine, err := Open(ctx, InMemory(), CacheSize(1<<20 /* 1 MiB */),
			func(cfg *engineConfig) error {
				// Latest cluster version, since these tests are not ones where we
				// are examining differences related to separated intents.
				cfg.Settings = cluster.MakeTestingClusterSettings()
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close()


	})
}
