// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sctest

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

// This file contains common logic for performing comparator testing
// between legacy and declarative schema changer.

type StmtLineReader interface {
	// HasNextLine returns true if there is more lines of SQL statements to process.
	HasNextLine() bool

	// NextLine retrieve the next line of SQL statements to process.
	NextLine() string
}

// CompareLegacyAndDeclarative is the core logic for performing comparator
// testing between legacy and declarative schema changer.
// It reads sql statements (mostly DDLs), one by one, from `ss` and execute them
// in a cluster using legacy schema changer and in another using declarative
// schema changer. It asserts that, if the statement fails, they must fail with
// a PG error with the same pg code. If the statement succeeded, all descriptors
// in the cluster should end up in the same state.
func CompareLegacyAndDeclarative(t *testing.T, ss StmtLineReader) {
	ctx := context.Background()

	legacyTSI, legacySQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer legacyTSI.Stopper().Stop(ctx)
	legacyTDB := sqlutils.MakeSQLRunner(legacySQLDB)
	legacyTDB.Exec(t, "SET use_declarative_schema_changer = off;")

	declarativeTSI, declarativeSQLDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer declarativeTSI.Stopper().Stop(ctx)
	declarativeTDB := sqlutils.MakeSQLRunner(declarativeSQLDB)
	declarativeTDB.Exec(t, "SET use_declarative_schema_changer = on;")

	for ss.HasNextLine() {
		line := ss.NextLine()
		_, errLegacy := legacySQLDB.Exec(line)
		if pgcode.MakeCode(string(getPQErrCode(errLegacy))) == pgcode.FeatureNotSupported {
			continue
		}
		_, errDeclarative := declarativeSQLDB.Exec(line)
		requireNoErrOrSameErrCode(t, line, errLegacy, errDeclarative)
	}
}

// requireNoErrOrSameErrCode require errors from executing some statement
// from legacy and declarative schema changer clusters to be both nil or
// both PQ error with same code.
func requireNoErrOrSameErrCode(t *testing.T, line string, errLegacy, errDeclarative error) {
	if errLegacy == nil && errDeclarative == nil {
		return
	}

	if errLegacy == nil {
		t.Fatalf("statement %q failed with declarative schema changer (but succeeded with legacy schema changer): %v", line, errDeclarative.Error())
	}
	if errDeclarative == nil {
		t.Fatalf("statement %q failed with legacy schema changer (but succeeded with declarative schema changer): %v", line, errLegacy.Error())
	}
	errLegacyPQCode := getPQErrCode(errLegacy)
	errDeclarativePQCode := getPQErrCode(errDeclarative)
	if errLegacyPQCode == "" || errDeclarativePQCode == "" {
		t.Fatalf("executing statement %q results in non-PQ error:  legacy=%v, declarative=%v ", line, errLegacy.Error(), errDeclarative.Error())
	}
	if errLegacyPQCode != errDeclarativePQCode {
		t.Fatalf("executing statement %q results in different error code: legacy=%v, declarative=%v", line, errLegacyPQCode, errDeclarativePQCode)
	}
}

func getPQErrCode(err error) pq.ErrorCode {
	if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
		return pqErr.Code
	}
	return ""
}
