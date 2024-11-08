// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcostserver_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/metrictestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)

		var ts testState
		ts.start(t)
		defer ts.stop()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			fn, ok := testStateCommands[d.Cmd]
			if !ok {
				d.Fatalf(t, "unknown command %s", d.Cmd)
			}
			return fn(&ts, t, d)
		})
	})
}

type testState struct {
	srv         serverutils.TestServerInterface
	s           serverutils.ApplicationLayerInterface
	db          *gosql.DB
	kvDB        *kv.DB
	r           *sqlutils.SQLRunner
	clock       *timeutil.ManualTime
	tenantUsage multitenant.TenantUsageServer
	metricsReg  *metric.Registry
	autoSeqNum  int64
}

const timeFormat = "15:04:05.000"

var t0 = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

func (ts *testState) start(t *testing.T) {
	// Set up a server that we use only for the system tables.
	ts.srv, ts.db, ts.kvDB = serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
	})
	ts.s = ts.srv.ApplicationLayer()
	ts.r = sqlutils.MakeSQLRunner(ts.db)

	ts.clock = timeutil.NewManualTime(t0)
	ts.tenantUsage = tenantcostserver.NewInstance(
		ts.s.ClusterSettings(),
		ts.kvDB,
		ts.s.InternalDB().(isql.DB),
		ts.clock,
	)
	ts.metricsReg = metric.NewRegistry()
	ts.metricsReg.AddMetricStruct(ts.tenantUsage.Metrics())
}

func (ts *testState) stop() {
	ts.srv.Stopper().Stop(context.Background())
}

func (ts *testState) formatTime(tm time.Time) string {
	return tm.Format(timeFormat)
}

var testStateCommands = map[string]func(*testState, *testing.T, *datadriven.TestData) string{
	"create-tenant":        (*testState).createTenant,
	"token-bucket-request": (*testState).tokenBucketRequest,
	"metrics":              (*testState).metrics,
	"configure":            (*testState).configure,
	"inspect":              (*testState).inspect,
	"wait-inspect":         (*testState).waitInspect,
	"advance":              (*testState).advance,
}

func (ts *testState) tenantID(t *testing.T, d *datadriven.TestData) uint64 {
	for _, arg := range d.CmdArgs {
		if arg.Key == "tenant" {
			if len(arg.Vals) != 1 {
				d.Fatalf(t, "tenant argument requires a value")
			}
			id, err := strconv.Atoi(arg.Vals[0])
			if err != nil || id < 1 {
				d.Fatalf(t, "invalid tenant argument")
			}
			return uint64(id)
		}
	}
	d.Fatalf(t, "command requires tenant=<id> argument")
	return 0
}

func (ts *testState) createTenant(t *testing.T, d *datadriven.TestData) string {
	ts.r.Exec(t, fmt.Sprintf("SELECT crdb_internal.create_tenant(%d)", ts.tenantID(t, d)))
	return ""
}

// tokenBucketRequest runs a TokenBucket request against a tenant (with ID
// specified in a tenant=X argument). The input is a yaml for the struct below.
func (ts *testState) tokenBucketRequest(t *testing.T, d *datadriven.TestData) string {
	tenantID := ts.tenantID(t, d)
	var args struct {
		InstanceID         uint32 `yaml:"instance_id"`
		InstanceLease      string `yaml:"instance_lease"`
		NextLiveInstanceID uint32 `yaml:"next_live_instance_id"`
		SeqNum             int64  `yaml:"seq_num"`
		Consumption        struct {
			RU                     float64 `yaml:"ru"`
			KVRU                   float64 `yaml:"kvru"`
			ReadBatches            uint64  `yaml:"read_batches"`
			ReadReq                uint64  `yaml:"read_req"`
			ReadBytes              uint64  `yaml:"read_bytes"`
			WriteBatches           uint64  `yaml:"write_batches"`
			WriteReq               uint64  `yaml:"write_req"`
			WriteBytes             uint64  `yaml:"write_bytes"`
			SQLPodsCPUSeconds      float64 `yaml:"sql_pods_cpu_seconds"`
			PGWireEgressBytes      uint64  `yaml:"pgwire_egress_bytes"`
			ExternalIOIngressBytes uint64  `yaml:"external_io_ingress_bytes"`
			ExternalIOEgressBytes  uint64  `yaml:"external_io_egress_bytes"`
			CrossRegionNetworkRU   float64 `yaml:"cross_region_network_ru"`
			EstimatedCPUSeconds    float64 `yaml:"estimated_cpu_seconds"`
		}
		ConsumptionPeriod string  `yaml:"consumption_period"`
		Tokens            float64 `yaml:"tokens"`
		RequestPeriod     string  `yaml:"request_period"`
	}
	args.SeqNum = -1
	args.ConsumptionPeriod = "10s"
	args.RequestPeriod = "10s"
	args.InstanceLease = "foo"
	if err := yaml.UnmarshalStrict([]byte(d.Input), &args); err != nil {
		d.Fatalf(t, "failed to parse request yaml: %v", err)
	}
	// If sequence number not specified, use an auto-incrementing number.
	if args.SeqNum == -1 {
		ts.autoSeqNum++
		args.SeqNum = ts.autoSeqNum
	}
	consumptionPeriod, err := time.ParseDuration(args.ConsumptionPeriod)
	if err != nil {
		d.Fatalf(t, "failed to parse duration: %v", args.ConsumptionPeriod)
	}
	requestPeriod, err := time.ParseDuration(args.RequestPeriod)
	if err != nil {
		d.Fatalf(t, "failed to parse duration: %v", args.RequestPeriod)
	}
	req := kvpb.TokenBucketRequest{
		TenantID:           tenantID,
		InstanceID:         args.InstanceID,
		InstanceLease:      []byte(args.InstanceLease),
		NextLiveInstanceID: args.NextLiveInstanceID,
		SeqNum:             args.SeqNum,
		ConsumptionSinceLastRequest: kvpb.TenantConsumption{
			RU:                     args.Consumption.RU,
			KVRU:                   args.Consumption.KVRU,
			ReadBatches:            args.Consumption.ReadBatches,
			ReadRequests:           args.Consumption.ReadReq,
			ReadBytes:              args.Consumption.ReadBytes,
			WriteBatches:           args.Consumption.WriteBatches,
			WriteRequests:          args.Consumption.WriteReq,
			WriteBytes:             args.Consumption.WriteBytes,
			SQLPodsCPUSeconds:      args.Consumption.SQLPodsCPUSeconds,
			PGWireEgressBytes:      args.Consumption.PGWireEgressBytes,
			ExternalIOIngressBytes: args.Consumption.ExternalIOIngressBytes,
			ExternalIOEgressBytes:  args.Consumption.ExternalIOEgressBytes,
			CrossRegionNetworkRU:   args.Consumption.CrossRegionNetworkRU,
			EstimatedCPUSeconds:    args.Consumption.EstimatedCPUSeconds,
		},
		ConsumptionPeriod:   consumptionPeriod,
		RequestedTokens:     args.Tokens,
		TargetRequestPeriod: requestPeriod,
	}
	res := ts.tenantUsage.TokenBucketRequest(
		context.Background(), roachpb.MustMakeTenantID(tenantID), &req,
	)
	if res.Error != (errors.EncodedError{}) {
		return fmt.Sprintf("error: %v", errors.DecodeError(context.Background(), res.Error))
	}
	if res.GrantedTokens == 0 {
		if res.TrickleDuration != 0 {
			d.Fatalf(t, "trickle duration set with 0 granted tokens")
		}
		return ""
	}
	trickleStr := "immediately"
	if res.TrickleDuration != 0 {
		trickleStr = fmt.Sprintf("over %s", res.TrickleDuration)
	}
	return fmt.Sprintf(
		"%.10g tokens granted %s. Fallback rate: %.10g tokens/s\n",
		res.GrantedTokens, trickleStr, res.FallbackRate,
	)
}

// metrics outputs all metrics that match the regex in the input.
func (ts *testState) metrics(t *testing.T, d *datadriven.TestData) string {
	re, err := regexp.Compile(d.Input)
	if err != nil {
		d.Fatalf(t, "failed to compile pattern: %v", err)
	}
	str, err := metrictestutils.GetMetricsText(ts.metricsReg, re)
	if err != nil {
		d.Fatalf(t, "failed to scrape metrics: %v", err)
	}
	return str
}

// configure reconfigures the token bucket for a tenant (specified in a tenant=X
// argument). The input is a yaml for the struct below.
func (ts *testState) configure(t *testing.T, d *datadriven.TestData) string {
	tenantID := ts.tenantID(t, d)
	var args struct {
		AvailableTokens float64 `yaml:"available_tokens"`
		RefillRate      float64 `yaml:"refill_rate"`
		MaxBurstTokens  float64 `yaml:"max_burst_tokens"`
	}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &args); err != nil {
		d.Fatalf(t, "failed to parse request yaml: %v", err)
	}
	db := ts.s.InternalDB().(isql.DB)
	if err := db.Txn(context.Background(), func(
		ctx context.Context, txn isql.Txn,
	) error {
		return ts.tenantUsage.ReconfigureTokenBucket(
			ctx,
			txn,
			roachpb.MustMakeTenantID(tenantID),
			args.AvailableTokens,
			args.RefillRate,
			args.MaxBurstTokens,
		)
	}); err != nil {
		d.Fatalf(t, "reconfigure error: %v", err)
	}
	return ""
}

// inspect shows all the metadata for a tenant (specified in a tenant=X
// argument), in a user-friendly format.
func (ts *testState) inspect(t *testing.T, d *datadriven.TestData) (res string) {
	tenantID := ts.tenantID(t, d)
	if err := ts.s.InternalDB().(isql.DB).Txn(context.Background(), func(
		ctx context.Context, txn isql.Txn,
	) (err error) {
		res, err = tenantcostserver.InspectTenantMetadata(
			context.Background(),
			txn,
			roachpb.MustMakeTenantID(tenantID),
			timeFormat,
		)
		return err
	}); err != nil {
		d.Fatalf(t, "error inspecting tenant state: %v", err)
	}
	return res
}

// inspect-wait is like inspect but waits a little bit and retries until the
// output equals the expected output; used for cases where
func (ts *testState) waitInspect(t *testing.T, d *datadriven.TestData) string {
	time.Sleep(1 * time.Millisecond)
	var output string
	err := testutils.SucceedsSoonError(func() error {
		res := ts.inspect(t, d)
		if res == d.Expected {
			output = res
			return nil
		}
		return errors.Errorf("-- expected:\n%s\n-- got:\n%s", d.Expected, res)
	})
	if err != nil {
		d.Fatalf(t, "error inspecting tenant state: %v", err)
	}
	return output
}

// advance advances the clock by the provided duration and returns the new
// current time.
func (ts *testState) advance(t *testing.T, d *datadriven.TestData) string {
	dur, err := time.ParseDuration(d.Input)
	if err != nil {
		d.Fatalf(t, "failed to parse input as duration: %v", err)
	}
	// This is hacky, but we want to be able to test the time going back which is
	// not allowed by ManualTime.Advance(). This is safe to do because we are not
	// using timers.
	*ts.clock = *timeutil.NewManualTime(ts.clock.Now().Add(dur))
	return ts.formatTime(ts.clock.Now())
}

// TestInstanceCleanup is a randomized test that verifies that the server keeps
// up with a changing live set.
func TestInstanceCleanup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var ts testState
	ts.start(t)
	defer ts.stop()

	ts.r.Exec(t, fmt.Sprintf("SELECT crdb_internal.create_tenant(%d)", 5))

	// Note: this number needs to be at most maxInstancesCleanup.
	const maxInstances = 10
	var liveset, prev intsets.Fast

	for steps := 0; steps < 100; steps++ {
		// Keep the previous set for debugging.
		prev = liveset.Copy()
		// Make a few random changes to the set.
		for n := rand.Intn(4); n >= 0; n-- {
			x := 1 + rand.Intn(maxInstances)
			if rand.Intn(2) == 0 {
				liveset.Add(x)
			} else {
				liveset.Remove(x)
			}
		}
		// Advance the time so all existing instances look stale.
		ts.clock.Advance(5 * time.Minute)
		if liveset.Empty() {
			// An empty live set can't trigger cleanup.
			continue
		}
		// Send one token bucket update from each instance, in random order.
		instances := liveset.Ordered()
		for _, i := range rand.Perm(len(instances)) {
			req := kvpb.TokenBucketRequest{
				TenantID:   5,
				InstanceID: uint32(instances[i]),
			}
			if i+1 < len(instances) {
				req.NextLiveInstanceID = uint32(instances[i+1])
			} else {
				req.NextLiveInstanceID = uint32(instances[0])
			}
			res := ts.tenantUsage.TokenBucketRequest(
				context.Background(), roachpb.MustMakeTenantID(5), &req,
			)
			if res.Error != (errors.EncodedError{}) {
				t.Fatal(errors.DecodeError(context.Background(), res.Error))
			}
		}
		// Verify that the server reached the correct liveset.
		rows := ts.r.Query(t,
			"SELECT instance_id FROM system.tenant_usage WHERE tenant_id = 5 AND instance_id > 0",
		)
		var serverSet intsets.Fast
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			serverSet.Add(id)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if !liveset.Equals(serverSet) {
			t.Fatalf(
				"previous live set: %s  current live set: %s  server live set: %s",
				liveset, prev, serverSet,
			)
		}
	}
}
