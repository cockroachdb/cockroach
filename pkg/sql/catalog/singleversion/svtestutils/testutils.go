// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package svtestutils contains utilities for testing singleversion
// implementation packages.
package svtestutils

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func SetUpTestingTable(
	t *testing.T, tdb *sqlutils.SQLRunner,
) (tableName string, tableID descpb.ID) {
	table := catconstants.SingleVersionDescriptorLeasesTableName
	tableName = fmt.Sprintf("%q.public.%s", t.Name(), table)
	createTable := strings.Replace(
		systemschema.SingleVersionDescriptorLeasesTableSchema,
		fmt.Sprintf("system.%s", table),
		tableName, 1,
	)
	tdb.Exec(t, fmt.Sprintf("CREATE DATABASE %q", t.Name()))
	tdb.Exec(t, createTable)
	tdb.QueryRow(
		t, "SELECT $1::regclass::int", tableName,
	).Scan(&tableID)
	return tableName, tableID
}

type TestInstance struct {
	mu struct {
		syncutil.Mutex
		session sqlliveness.Session
	}
}

func (t *TestInstance) SetSession(session sqlliveness.Session) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.session = session
}

func (t *TestInstance) Session(ctx context.Context) (sqlliveness.Session, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.mu.session, nil
}

func MakeTestSession(id sqlliveness.SessionID, exp hlc.Timestamp) sqlliveness.Session {
	return &testSession{
		id:         id,
		expiration: exp,
	}
}

type testSession struct {
	id         sqlliveness.SessionID
	expiration hlc.Timestamp
}

func (t testSession) ID() sqlliveness.SessionID {
	return t.id
}

func (t testSession) Expiration() hlc.Timestamp {
	return t.expiration
}

func (t testSession) RegisterCallbackForSessionExpiry(f func(ctx context.Context)) {
	panic("implement me")
}

var _ sqlliveness.Instance = (*TestInstance)(nil)
var _ sqlliveness.Session = (*testSession)(nil)
