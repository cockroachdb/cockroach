// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"time"

	"github.com/mitchellh/reflectwalk"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const baseUpdatesURL = `https://register.cockroachdb.com/api/clusters/updates`
const baseReportingURL = `https://register.cockroachdb.com/api/clusters/report`

var updatesURL, reportingURL *url.URL

func init() {
	var err error
	updatesURL, err = url.Parse(
		envutil.EnvOrDefaultString("COCKROACH_UPDATE_CHECK_URL", baseUpdatesURL),
	)
	if err != nil {
		panic(err)
	}
	reportingURL, err = url.Parse(
		envutil.EnvOrDefaultString("COCKROACH_USAGE_REPORT_URL", baseReportingURL),
	)
	if err != nil {
		panic(err)
	}
}

const (
	updateCheckFrequency = time.Hour * 24
	// TODO(dt): switch to settings.
	updateCheckPostStartup    = time.Minute * 5
	updateCheckRetryFrequency = time.Hour
	updateMaxVersionsToReport = 3

	updateCheckJitterSeconds = 120
)

var (
	diagnosticReportFrequency = settings.RegisterDurationSetting(
		"diagnostics.reporting.interval",
		"interval at which diagnostics data should be reported",
		time.Hour,
	)

	// TODO(dt): this should be split from the report interval.
	// statsResetFrequency = settings.RegisterDurationSetting(
	// 	"sql.metrics.statement_details.reset_interval",
	// 	"interval at which the collected statement statistics should be reset",
	// 	time.Hour,
	// )
)

// randomly shift `d` to be up to `jitterSec` shorter or longer.
func addJitter(d time.Duration, jitterSec int) time.Duration {
	j := time.Duration(rand.Intn(jitterSec*2)-jitterSec) * time.Second
	return d + j
}

type versionInfo struct {
	Version string `json:"version"`
	Details string `json:"details"`
}

type reportingInfo struct {
	Node       nodeInfo                                          `json:"node"`
	Stores     []storeInfo                                       `json:"stores"`
	Schema     []sqlbase.TableDescriptor                         `json:"schema"`
	QueryStats map[string]map[string]roachpb.StatementStatistics `json:"sqlstats"`
}

type nodeInfo struct {
	NodeID     roachpb.NodeID `json:"node_id"`
	Bytes      int            `json:"bytes"`
	KeyCount   int            `json:"key_count"`
	RangeCount int            `json:"range_count"`
}

type storeInfo struct {
	NodeID     roachpb.NodeID  `json:"node_id"`
	StoreID    roachpb.StoreID `json:"store_id"`
	Bytes      int             `json:"bytes"`
	KeyCount   int             `json:"key_count"`
	RangeCount int             `json:"range_count"`
}

// PeriodicallyCheckForUpdates starts a background worker that periodically
// phones home to check for updates and report usage.
func (s *Server) PeriodicallyCheckForUpdates() {
	s.stopper.RunWorker(context.TODO(), func(ctx context.Context) {
		startup := timeutil.Now()
		nextUpdateCheck := startup
		nextDiagnosticReport := startup

		var timer timeutil.Timer
		defer timer.Stop()
		for {
			now := timeutil.Now()
			runningTime := now.Sub(startup)

			nextUpdateCheck = s.maybeCheckForUpdates(now, nextUpdateCheck, runningTime)
			nextDiagnosticReport = s.maybeReportDiagnostics(ctx, now, nextDiagnosticReport, runningTime)

			sooner := nextUpdateCheck
			if nextDiagnosticReport.Before(sooner) {
				sooner = nextDiagnosticReport
			}

			timer.Reset(addJitter(sooner.Sub(timeutil.Now()), updateCheckJitterSeconds))
			select {
			case <-s.stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
			}
		}
	})
}

// maybeCheckForUpdates determines if it is time to check for updates and does
// so if it is, before returning the time at which the next check be done.
func (s *Server) maybeCheckForUpdates(
	now, scheduled time.Time, runningTime time.Duration,
) time.Time {
	if scheduled.After(now) {
		return scheduled
	}

	// checkForUpdates handles its own errors, but it returns a bool indicating if
	// it succeeded, so we can schedule a re-attempt if it did not.
	if succeeded := s.checkForUpdates(runningTime); !succeeded {
		return now.Add(updateCheckRetryFrequency)
	}

	// If we've just started up, we want to check again shortly after.
	// During startup is when a message is most likely to be actually seen by a
	// human operator so we check as early as possible, but this makes it hard to
	// differentiate real deployments vs short-lived instances for tests.
	if runningTime < updateCheckPostStartup {
		return now.Add(time.Hour - runningTime)
	}

	return now.Add(updateCheckFrequency)
}

// checkForUpdates calls home to check for new versions for the current platform
// and logs messages if it finds them, as well as if it encounters any errors.
// The returned boolean indicates if the check succeeded (and thus does not need
// to be re-attempted by the scheduler after a retry-interval).
func (s *Server) checkForUpdates(runningTime time.Duration) bool {
	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "checkForUpdates")
	defer span.Finish()

	q := updatesURL.Query()
	b := build.GetInfo()
	q.Set("version", b.Tag)
	q.Set("platform", b.Platform)
	q.Set("uuid", s.node.ClusterID.String())
	q.Set("nodeid", s.NodeID().String())
	q.Set("uptime", strconv.Itoa(int(runningTime.Seconds())))
	updatesURL.RawQuery = q.Encode()

	res, err := http.Get(updatesURL.String())
	if err != nil {
		// This is probably going to be relatively common in production
		// environments where network access is usually curtailed.
		return false
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(res.Body)
		log.Warningf(ctx, "failed to check for updates: status: %s, body: %s, error: %v",
			res.Status, b, err)
		return false
	}

	decoder := json.NewDecoder(res.Body)
	r := struct {
		Details []versionInfo `json:"details"`
	}{}

	err = decoder.Decode(&r)
	if err != nil && err != io.EOF {
		log.Warning(ctx, "Error decoding updates info: ", err)
		return false
	}

	// Ideally the updates server only returns the most relevant updates for us,
	// but if it replied with an excessive number of updates, limit log spam by
	// only printing the last few.
	if len(r.Details) > updateMaxVersionsToReport {
		r.Details = r.Details[len(r.Details)-updateMaxVersionsToReport:]
	}
	for _, v := range r.Details {
		log.Infof(ctx, "A new version is available: %s, details: %s", v.Version, v.Details)
	}
	return true
}

var diagnosticsMetricsEnabled = settings.RegisterBoolSetting(
	"diagnostics.reporting.report_metrics",
	"enable collection and reporting diagnostic metrics to cockroach labs",
	true,
)

func (s *Server) maybeReportDiagnostics(
	ctx context.Context, now, scheduled time.Time, running time.Duration,
) time.Time {
	if scheduled.After(now) {
		return scheduled
	}

	// TODO(dt): we should allow tuning the reset and report intervals separately.
	// Consider something like rand.Float() > resetFreq/reportFreq here to sample
	// stat reset periods for reporting.
	if log.DiagnosticsReportingEnabled.Get() && diagnosticsMetricsEnabled.Get() {
		s.reportDiagnostics(running)
	}
	s.sqlExecutor.ResetStatementStats(ctx)

	return scheduled.Add(diagnosticReportFrequency.Get())
}

func (s *Server) getReportingInfo(ctx context.Context) reportingInfo {
	n := s.node.recorder.GetStatusSummary()

	summary := nodeInfo{NodeID: s.node.Descriptor.NodeID}

	stores := make([]storeInfo, len(n.StoreStatuses))
	for i, r := range n.StoreStatuses {
		stores[i].NodeID = r.Desc.Node.NodeID
		stores[i].StoreID = r.Desc.StoreID
		stores[i].KeyCount = int(r.Metrics["keycount"])
		summary.KeyCount += stores[i].KeyCount
		stores[i].RangeCount = int(r.Metrics["replicas"])
		summary.RangeCount += stores[i].RangeCount
		bytes := int(r.Metrics["sysbytes"] + r.Metrics["intentbytes"] + r.Metrics["valbytes"] + r.Metrics["keybytes"])
		stores[i].Bytes = bytes
		summary.Bytes += bytes
	}

	schema, err := s.collectSchemaInfo(ctx)
	if err != nil {
		log.Warningf(ctx, "error collecting schema info for diagnostic report: %+v", err)
		schema = nil
	}

	return reportingInfo{summary, stores, schema, s.sqlExecutor.GetScrubbedStmtStats()}
}

func (s *Server) reportDiagnostics(runningTime time.Duration) {
	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "usageReport")
	defer span.Finish()

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(s.getReportingInfo(ctx)); err != nil {
		log.Warning(ctx, err)
		return
	}

	q := reportingURL.Query()
	q.Set("version", build.GetInfo().Tag)
	q.Set("uuid", s.node.ClusterID.String())
	q.Set("uptime", strconv.Itoa(int(runningTime.Seconds())))
	reportingURL.RawQuery = q.Encode()

	res, err := http.Post(reportingURL.String(), "application/json", b)

	if err != nil {
		if log.V(2) {
			// This is probably going to be relatively common in production
			// environments where network access is usually curtailed.
			log.Warning(ctx, "failed to report node usage metrics: ", err)
		}
		return
	}

	if res.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(res.Body)
		log.Warningf(ctx, "failed to report node usage metrics: status: %s, body: %s, "+
			"error: %v", res.Status, b, err)
	}
}

func (s *Server) collectSchemaInfo(ctx context.Context) ([]sqlbase.TableDescriptor, error) {
	startKey := roachpb.Key(keys.MakeTablePrefix(keys.DescriptorTableID))
	endKey := startKey.PrefixEnd()
	kvs, err := s.db.Scan(ctx, startKey, endKey, 0)
	if err != nil {
		return nil, err
	}
	tables := make([]sqlbase.TableDescriptor, 0, len(kvs))
	redactor := stringRedactor{}
	for _, kv := range kvs {
		var desc sqlbase.Descriptor
		if err := kv.ValueProto(&desc); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", kv.Key)
		}
		if t := desc.GetTable(); t != nil && t.ID > keys.MaxReservedDescID {
			if err := reflectwalk.Walk(t, redactor); err != nil {
				panic(err) // stringRedactor never returns a non-nil err
			}
			tables = append(tables, *t)
		}
	}
	return tables, nil
}

type stringRedactor struct{}

func (stringRedactor) Primitive(v reflect.Value) error {
	if v.Kind() == reflect.String && v.String() != "" {
		v.Set(reflect.ValueOf("_"))
	}
	return nil
}
