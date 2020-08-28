// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// TestDrainingNamesAreCleanedTypeChangeOnFailure ensures that draining names
// are cleaned up if the type schema change job runs into a failure in Resume().
func TestDrainingNamesAreCleanedOnTypeChangeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Decrease the adopt loop interval so that retries happen quickly.
	defer setTestJobsAdoptInterval()()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			return errors.New("boom")
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create a type.
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TYPE d.t AS ENUM()
`); err != nil {
		t.Fatal(err)
	}

	// Try a rename. This should fail with "boom".
	_, err := sqlDB.Exec(`ALTER TYPE d.t RENAME TO t2`)
	if err == nil {
		t.Fatal("expected error, found nil")
	}
	if !testutils.IsError(err, "boom") {
		t.Fatalf("expected boom, found %v", err)
	}

	// The failure hook should kick in and drain the names.
	testutils.SucceedsSoon(t, func() error {
		_, err := sqlDB.Exec(`CREATE TYPE d.t AS ENUM ('drained')`)
		return err
	})
}

// TestTypeSchemaChangeHandlesDeletedDescriptor ensures that the type schema
// change process is resilient to deleted descriptors.
func TestTypeSchemaChangeHandlesDeletedDescriptor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var delTypeDesc func()
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			delTypeDesc()
			return nil
		},
	}

	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create a type.
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TYPE d.t AS ENUM();
`); err != nil {
		t.Fatal(err)
	}

	// Set up delTypeDesc to delete t.
	desc := catalogkv.TestingGetTypeDescriptor(kvDB, keys.SystemSQLCodec, "d", "t")
	delTypeDesc = func() {
		// Delete the descriptor.
		if err := kvDB.Del(ctx, catalogkeys.MakeDescMetadataKey(keys.SystemSQLCodec, desc.ID)); err != nil {
			t.Error(err)
		}
	}

	// A job running on this descriptor shouldn't fail horribly.
	if _, err := sqlDB.Exec(`ALTER TYPE d.t RENAME TO t2`); err != nil {
		t.Fatal(err)
	}
}

// TestTypeSchemaChangeRetriesTransparently tests that a type schema change
// that runs into a non permanent error will retry transparently.
func TestTypeSchemaChangeRetriesTransparently(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Decrease the adopt loop interval so that retries happen quickly.
	defer setTestJobsAdoptInterval()()

	ctx := context.Background()
	// Protects errorReturned.
	var mu syncutil.Mutex
	errorReturned := false
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLTypeSchemaChanger = &sql.TypeSchemaChangerTestingKnobs{
		RunBeforeExec: func() error {
			mu.Lock()
			defer mu.Unlock()
			// Return a retryable error on the first call.
			if errorReturned {
				return nil
			}
			errorReturned = true
			return context.DeadlineExceeded
		},
	}

	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create a type.
	if _, err := sqlDB.Exec(`
CREATE DATABASE d;
CREATE TYPE d.t AS ENUM();
`); err != nil {
		t.Fatal(err)
	}

	// The retry should happen within the job and succeed.
	if _, err := sqlDB.Exec(`ALTER TYPE d.t RENAME TO t2`); err != nil {
		t.Fatal(err)
	}
}
