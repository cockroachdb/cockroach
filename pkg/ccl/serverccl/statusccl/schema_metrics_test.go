// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package statusccl

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/metrictestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestTenantSchemaMetrics_DatabaseCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()
	helper := newTestTenantHelper(t, 3, base.TestingKnobs{})
	defer helper.cleanup(ctx, t)

	storage, cleanup := makeStorageServer()
	defer cleanup()

	sql0 := helper.testCluster().tenantConn(0)
	sql1 := helper.testCluster().tenantConn(1)
	sql2 := helper.testCluster().tenantConn(2)

	http0 := helper.testCluster().tenantHTTPClient(t, 0)
	defer http0.Close()
	http1 := helper.testCluster().tenantHTTPClient(t, 1)
	defer http0.Close()
	http2 := helper.testCluster().tenantHTTPClient(t, 2)
	defer http0.Close()

	maxUpdatedAt := scrapingMetricsFrom(t, http0, http1, http2).
		LabelingSources("sql_instance_id", strconv.Itoa).
		Gauge("sql_schema_updated_at").
		Max()

	databaseCount := scrapingMetricsFrom(t, http0, http1, http2).
		LabelingSources("sql_instance_id", strconv.Itoa).
		Gauge("sql_schema_database_count").
		WithLabel("internal", "false").
		WithLabelf("sql_instance_id", maxUpdatedAt.Label, "sql_instance_id").
		Single()

	databaseCount.ShouldEventuallyEqual(1) // defaultdb

	sql0.Exec(t, "CREATE DATABASE foo")
	databaseCount.ShouldEventuallyEqual(2) // defaultdb, foo

	sql1.Exec(t, "BACKUP DATABASE foo INTO $1", storage.Path())
	sql1.Exec(t, "DROP DATABASE foo")
	databaseCount.ShouldEventuallyEqual(1) // defaultdb

	sql2.Exec(t, "RESTORE DATABASE foo FROM $1", storage.Path())
	databaseCount.ShouldEventuallyEqual(2) // defaultdb, foo

	sql0.Exec(t, "ALTER DATABASE foo CONVERT TO SCHEMA WITH PARENT defaultdb")
	databaseCount.ShouldEventuallyEqual(1) // defaultdb
}

func TestTenantSchemaMetrics_TableCount(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStressRace(t, "expensive tests")

	ctx := context.Background()
	helper := newTestTenantHelper(t, 3, base.TestingKnobs{})
	defer helper.cleanup(ctx, t)

	storage, cleanup := makeStorageServer()
	defer cleanup()

	sql0 := helper.testCluster().tenantConn(0)
	sql1 := helper.testCluster().tenantConn(1)
	sql2 := helper.testCluster().tenantConn(2)

	http0 := helper.testCluster().tenantHTTPClient(t, 0)
	defer http0.Close()
	http1 := helper.testCluster().tenantHTTPClient(t, 1)
	defer http1.Close()
	http2 := helper.testCluster().tenantHTTPClient(t, 2)
	defer http2.Close()

	maxUpdatedAt := scrapingMetricsFrom(t, http0, http1, http2).
		LabelingSources("sql_instance_id", strconv.Itoa).
		Gauge("sql_schema_updated_at").
		Max()

	tableCount := scrapingMetricsFrom(t, http0, http1, http2).
		LabelingSources("sql_instance_id", strconv.Itoa).
		Gauge("sql_schema_table_count").
		WithLabel("internal", "false").
		WithLabelf("sql_instance_id", maxUpdatedAt.Label, "sql_instance_id").
		Single()

	tableCount.ShouldEventuallyEqual(0)

	sql0.Exec(t, "CREATE TABLE foo (id INT)")
	tableCount.ShouldEventuallyEqual(1)

	sql1.Exec(t, "DROP TABLE foo")
	tableCount.ShouldEventuallyEqual(0)

	storage.Seed("pgdump", "create table bar (id int); insert into bar (id) values (1);")
	sql2.Exec(t, "IMPORT TABLE bar FROM PGDUMP $1", storage.Path("pgdump"))
	tableCount.ShouldEventuallyEqual(1)
}

func scrapingMetricsFrom(t *testing.T, clients ...*httpClient) *metrictestutils.MetricsTester {
	var loaders []func() string
	for _, client := range clients {
		client := client
		loaders = append(loaders, func() string {
			return client.GetPlainText("/_status/vars")
		})
	}
	return metrictestutils.NewMetricsTester(t, loaders...)
}

type storageServer struct {
	blobs  map[string][]byte
	server *httptest.Server
}

func makeStorageServer() (*storageServer, func()) {
	s := storageServer{
		blobs: map[string][]byte{},
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.serve))
	return &s, s.server.Close
}

func (s *storageServer) Path(segments ...string) string {
	return strings.Join([]string{s.server.URL, strings.Join(segments, "/")}, "/")
}

func (s *storageServer) Seed(path, content string) {
	s.blobs[path] = []byte(content)
}

func (s *storageServer) serve(w http.ResponseWriter, r *http.Request) {
	path := filepath.Base(r.URL.Path)

	switch r.Method {
	case "PUT":
		s.blobs[path], _ = io.ReadAll(r.Body)
		w.WriteHeader(201)
	case "GET", "HEAD":
		blob, ok := s.blobs[path]
		if !ok {
			http.Error(w, path, 404)
			return
		}
		http.ServeContent(w, r, path, time.Time{}, bytes.NewReader(blob))
	case "DELETE":
		delete(s.blobs, path)
		w.WriteHeader(204)
	default:
		http.Error(w, fmt.Sprintf("unsupported method %s", r.Method), 400)
	}
}
