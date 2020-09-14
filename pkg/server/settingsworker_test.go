// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/openzipkin-contrib/zipkin-go-opentracing/thrift/gen-go/zipkincore"
	"github.com/stretchr/testify/assert"
)

const strKey = "testing.str"
const intKey = "testing.int"
const durationKey = "testing.duration"
const byteSizeKey = "testing.bytesize"
const enumKey = "testing.enum"

var strA = settings.RegisterValidatedStringSetting(strKey, "desc", "<default>", func(sv *settings.Values, v string) error {
	if len(v) > 15 {
		return errors.Errorf("can't set %s to string longer than 15: %s", strKey, v)
	}
	return nil
})
var intA = settings.RegisterValidatedIntSetting(intKey, "desc", 1, func(v int64) error {
	if v < 0 {
		return errors.Errorf("can't set %s to a negative value: %d", intKey, v)
	}
	return nil

})
var durationA = settings.RegisterValidatedDurationSetting(durationKey, "desc", time.Minute, func(v time.Duration) error {
	if v < 0 {
		return errors.Errorf("can't set %s to a negative duration: %s", durationKey, v)
	}
	return nil
})
var byteSizeA = settings.RegisterValidatedByteSizeSetting(byteSizeKey, "desc", 1024*1024, func(v int64) error {
	if v < 0 {
		return errors.Errorf("can't set %s to a negative value: %d", byteSizeKey, v)
	}
	return nil
})
var enumA = settings.RegisterEnumSetting(enumKey, "desc", "foo", map[int64]string{1: "foo", 2: "bar"})

func TestSettingsRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set up some additional cluster settings to play around with. Note that we
	// need to do this before starting the server, or there will be data races.
	st := cluster.MakeTestingClusterSettings()
	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(rawDB)

	insertQ := `UPSERT INTO system.settings (name, value, "lastUpdated", "valueType")
		VALUES ($1, $2, now(), $3)`
	deleteQ := "DELETE FROM system.settings WHERE name = $1"

	if expected, actual := "<default>", strA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := int64(1), intA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	// Inserting a new setting is reflected in cache.
	db.Exec(t, insertQ, strKey, "foo", "s")
	db.Exec(t, insertQ, intKey, settings.EncodeInt(2), "i")
	// Wait until we observe the gossip-driven update propagating to cache.
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "foo", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(2), intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Setting to empty also works.
	db.Exec(t, insertQ, strKey, "", "s")
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// An unknown value doesn't block updates to a known one.
	db.Exec(t, insertQ, "dne", "???", "s")
	db.Exec(t, insertQ, strKey, "qux", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "qux", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := int64(2), intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A malformed value doesn't revert previous set or block other changes.
	db.Exec(t, deleteQ, "dne")
	db.Exec(t, insertQ, intKey, "invalid", "i")
	db.Exec(t, insertQ, strKey, "after-invalid", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(2), intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-invalid", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// A mis-typed value doesn't revert a previous set or block other changes.
	db.Exec(t, insertQ, intKey, settings.EncodeInt(7), "b")
	db.Exec(t, insertQ, strKey, "after-mistype", "s")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := int64(2), intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := "after-mistype", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// An invalid value doesn't revert a previous set or block other changes.
	prevStrA := strA.Get(&st.SV)
	prevIntA := intA.Get(&st.SV)
	prevDurationA := durationA.Get(&st.SV)
	prevByteSizeA := byteSizeA.Get(&st.SV)
	db.Exec(t, insertQ, strKey, "this is too big for this setting", "s")
	db.Exec(t, insertQ, intKey, settings.EncodeInt(-1), "i")
	db.Exec(t, insertQ, durationKey, settings.EncodeDuration(-time.Minute), "d")
	db.Exec(t, insertQ, byteSizeKey, settings.EncodeInt(-1), "z")

	testutils.SucceedsSoon(t, func() error {
		if expected, actual := prevStrA, strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := prevIntA, intA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := prevDurationA, durationA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		if expected, actual := prevByteSizeA, byteSizeA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

	// Deleting a value reverts to default.
	db.Exec(t, deleteQ, strKey)
	testutils.SucceedsSoon(t, func() error {
		if expected, actual := "<default>", strA.Get(&st.SV); expected != actual {
			return errors.Errorf("expected %v, got %v", expected, actual)
		}
		return nil
	})

}

func TestSettingsSetAndShow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// Set up some additional cluster settings to play around with. Note that we
	// need to do this before starting the server, or there will be data races.
	st := cluster.MakeTestingClusterSettings()
	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(rawDB)

	// TODO(dt): add placeholder support to SET and SHOW.
	setQ := `SET CLUSTER SETTING "%s" = %s`
	showQ := `SHOW CLUSTER SETTING "%s"`

	db.Exec(t, fmt.Sprintf(setQ, strKey, "'via-set'"))
	if expected, actual := "via-set", db.QueryStr(t, fmt.Sprintf(showQ, strKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, intKey, "5"))
	if expected, actual := "5", db.QueryStr(t, fmt.Sprintf(showQ, intKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, durationKey, "'2h'"))
	if expected, actual := time.Hour*2, durationA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "02:00:00", db.QueryStr(t, fmt.Sprintf(showQ, durationKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, byteSizeKey, "'1500MB'"))
	if expected, actual := int64(1500000000), byteSizeA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "1.4 GiB", db.QueryStr(t, fmt.Sprintf(showQ, byteSizeKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, byteSizeKey, "'1450MB'"))
	if expected, actual := "1.4 GiB", db.QueryStr(t, fmt.Sprintf(showQ, byteSizeKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.ExpectErr(t, `could not parse "a-str" as type int`, fmt.Sprintf(setQ, intKey, "'a-str'"))

	db.Exec(t, fmt.Sprintf(setQ, enumKey, "2"))
	if expected, actual := int64(2), enumA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "bar", db.QueryStr(t, fmt.Sprintf(showQ, enumKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.Exec(t, fmt.Sprintf(setQ, enumKey, "'foo'"))
	if expected, actual := int64(1), enumA.Get(&st.SV); expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
	if expected, actual := "foo", db.QueryStr(t, fmt.Sprintf(showQ, enumKey))[0][0]; expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}

	db.ExpectErr(
		t, `invalid string value 'unknown' for enum setting`,
		fmt.Sprintf(setQ, enumKey, "'unknown'"),
	)

	db.ExpectErr(t, `invalid integer value '7' for enum setting`, fmt.Sprintf(setQ, enumKey, "7"))
}

func TestSettingsShowAll(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set up some additional cluster settings to play around with. Note that we
	// need to do this before starting the server, or there will be data races.
	st := cluster.MakeTestingClusterSettings()

	s, rawDB, _ := serverutils.StartServer(t, base.TestServerArgs{Settings: st})
	defer s.Stopper().Stop(context.Background())

	db := sqlutils.MakeSQLRunner(rawDB)

	rows := db.QueryStr(t, "SHOW ALL CLUSTER SETTINGS")
	if len(rows) < 2 {
		t.Fatalf("show all returned too few rows (%d)", len(rows))
	}
	const expColumns = 5
	if len(rows[0]) != expColumns {
		t.Fatalf("show all must return %d columns, found %d", expColumns, len(rows[0]))
	}
	hasIntKey := false
	hasStrKey := false
	for _, row := range rows {
		switch row[0] {
		case strKey:
			hasStrKey = true
		case intKey:
			hasIntKey = true
		}
	}
	if !hasIntKey || !hasStrKey {
		t.Fatalf("show all did not find the test keys: %q", rows)
	}
}

// TestSettingsZipkinBatchSize tests that a batch size of 1
// can be configured for the zipkin trace collector
func TestSettingsZipkinBatchSize(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up collector handler function and batch size reporting
	type CollectorReport struct {
		ObservedBatchSize int
		Error             error
	}
	batchSizeReportingChan := make(chan CollectorReport, 1)
	sendObservedBatchSizeReport := func(observedBatchSize int, err error) {
		report := CollectorReport{
			ObservedBatchSize: observedBatchSize,
			Error:             err,
		}
		select {
		case batchSizeReportingChan <- report:
		default:
		}
	}

	collectorHandlerFcn := func(rw http.ResponseWriter, req *http.Request) {
		bodyBytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			sendObservedBatchSizeReport(-1, err)
			return
		}
		t := thrift.NewTMemoryBuffer()
		t.Write(bodyBytes)
		p := thrift.NewTBinaryProtocolTransport(t)
		_, batchSize, err := p.ReadListBegin()
		if err != nil {
			sendObservedBatchSizeReport(-1, err)
			return
		}

		sendObservedBatchSizeReport(batchSize, nil)
	}
	traceCollectorServer := httptest.NewServer(http.HandlerFunc(collectorHandlerFcn))
	traceCollectorServerURL, err := url.Parse(traceCollectorServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer traceCollectorServer.Close()

	// Start test cluster containing a test db and test table
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	execStatement := func(statement string) {
		if _, err := tc.ServerConn(rand.Intn(3)).Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	// Configure tracing with a batch size of 1 in the test cluster
	execStatement(`SET CLUSTER SETTING trace.zipkin.collector.batch.size = 1`)
	execStatement(fmt.Sprintf(`SET CLUSTER SETTING trace.zipkin.collector = '%s'`, traceCollectorServerURL.Host))

	// Run statements to be traced
	execStatement(`CREATE DATABASE t`)
	execStatement(`CREATE TABLE test (k INT PRIMARY KEY, v INT)`)
	execStatement(`INSERT INTO test VALUES (1,1)`)

	// Test that the collector observed a batch size of 1 with no error
	observedBatchReport := <-batchSizeReportingChan
	if observedBatchReport.Error != nil {
		t.Fatal(err)
	}
	assert.Equal(t, observedBatchReport.ObservedBatchSize, 1)
}

// TestSettingsTraceSamplingRate tests that setting the sampling rate
// of traces will correctly limit the percentage of traces sampled.
func TestSettingsTraceSamplingRate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Start test cluster containing a test db and test table
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	execStatement := func(statement string) {
		if _, err := tc.ServerConn(rand.Intn(3)).Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	execStatement(`CREATE DATABASE t`)
	execStatement(`CREATE TABLE test (k INT PRIMARY KEY, v INT)`)

	// Set up tracing collector and variable to count the number of traces
	// collected from INSERT INTO statements
	nTraces := int64(0)
	collectorHandlerFcn := func(rw http.ResponseWriter, req *http.Request) {
		bodyBytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			return
		}
		t := thrift.NewTMemoryBuffer()
		if _, err = t.Write(bodyBytes); err != nil {
			return
		}
		p := thrift.NewTBinaryProtocolTransport(t)
		_, batchSize, err := p.ReadListBegin()
		if err != nil {
			return
		}

		for i := 0; i < batchSize; i += 1 {
			span := zipkincore.Span{}
			if err = span.Read(p); err != nil {
				return
			}

			if span.Name == `exec stmt` {
				for _, annotation := range span.Annotations {
					if strings.Contains(annotation.Value, `INSERT INTO test VALUES`) &&
						strings.Contains(annotation.Value, `in state: Open`) {
						atomic.AddInt64(&nTraces, 1)
					}
				}
			}
		}
	}
	traceCollectorServer := httptest.NewServer(http.HandlerFunc(collectorHandlerFcn))
	defer traceCollectorServer.Close()

	// Configure tracing in the test cluster
	traceCollectorServerURL, err := url.Parse(traceCollectorServer.URL)
	if err != nil {
		t.Fatal(err)
	}
	setClusterTraceSamplingRate := func(rate float64) {
		execStatement(fmt.Sprintf(`SET CLUSTER SETTING trace.zipkin.sampling.rate = %f`, rate))
	}
	execStatement(fmt.Sprintf(`SET CLUSTER SETTING trace.zipkin.collector = '%s'`, traceCollectorServerURL.Host))

	// Function that writes 1000 rows
	startKey := 0
	write1000Rows := func() {
		waitGroup := sync.WaitGroup{}
		for key := startKey; key < startKey+1000; key += 10 {
			waitGroup.Add(1)
			go func(wg *sync.WaitGroup, k int) {
				defer wg.Done()
				for v := 0; v < 10; v++ {
					execStatement(fmt.Sprintf(`INSERT INTO test VALUES (%d, %d)`, k+v, v))
				}
			}(&waitGroup, key)
		}
		waitGroup.Wait()
		startKey = startKey + 1000
	}

	// Test 100% Sampling Rate
	setClusterTraceSamplingRate(1.0)
	nTraces = 0
	write1000Rows()
	testutils.SucceedsSoon(t, func() error {
		currentTraces := atomic.LoadInt64(&nTraces)
		if currentTraces != 1000 {
			return fmt.Errorf(`could not trace all SQL 'INSERT INTO' statements. traced %d out of 10000`, currentTraces)
		}
		return nil
	})

	// Test 50% Sampling Rate
	setClusterTraceSamplingRate(0.5)
	nTraces = 0
	write1000Rows()
	testutils.SucceedsSoon(t, func() error {
		currentTraces := atomic.LoadInt64(&nTraces)
		if currentTraces < 450 && currentTraces > 550 {
			return fmt.Errorf(`could not trace half of SQL 'INSERT INTO' statements. traced %d out of 10000`, currentTraces)
		}
		return nil
	})
}
