// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachanger_test

import (
	"flag"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/sctest"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Used for saving corpus information in TestGenerateCorpus
var corpusPath string

func init() {
	flag.StringVar(&corpusPath, "declarative-corpus", "", "Path to the corpus file")
}

func TestSchemaChangerSideEffects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	sctest.EndToEndSideEffects(t, testutils.TestDataPath(t), sctest.SingleNodeCluster)
}

func TestRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sctest.Rollback(t, testutils.TestDataPath(t), sctest.SingleNodeCluster)
}

func TestPause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	sctest.Pause(t, testutils.TestDataPath(t), sctest.SingleNodeCluster)
}

// TestGenerateCorpus generates a corpus based on the end to end test files.
func TestGenerateCorpus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	if corpusPath == "" {
		skip.IgnoreLintf(t, "requires decalarative-corpus path parameter")
	}
	sctest.GenerateSchemaChangeCorpus(t, testutils.TestDataPath(t), corpusPath, sctest.SingleNodeCluster)
}
