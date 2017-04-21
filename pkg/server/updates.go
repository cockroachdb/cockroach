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
	"strconv"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const baseUpdatesURL = `https://register.cockroachdb.com/api/clusters/updates`
const baseReportingURL = `https://register.cockroachdb.com/api/report`

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
	diagnosticReportFrequency = updateCheckFrequency
	updateCheckPostStartup    = time.Minute * 5
	updateCheckRetryFrequency = time.Hour
	updateMaxVersionsToReport = 3

	updateCheckJitterSeconds = 120
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
	Node   nodeInfo    `json:"node"`
	Stores []storeInfo `json:"stores"`
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
	s.stopper.RunWorker(context.TODO(), func(context.Context) {
		startup := timeutil.Now()
		var nextUpdateCheck, nextDiagnosticReport time.Time

		var timer timeutil.Timer
		defer timer.Stop()
		for {
			runningTime := timeutil.Since(startup)

			nextUpdateCheck = s.maybeCheckForUpdates(nextUpdateCheck, runningTime)
			nextDiagnosticReport = s.maybeReportDiagnostics(nextDiagnosticReport, runningTime)

			sooner := nextUpdateCheck
			if nextDiagnosticReport.Before(nextUpdateCheck) {
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
func (s *Server) maybeCheckForUpdates(scheduled time.Time, runningTime time.Duration) time.Time {
	if scheduled.After(timeutil.Now()) {
		return scheduled
	}

	// checkForUpdates handles its own errors, but it returns a bool indicating if
	// it succeeded, so we can schedule a re-attempt if it did not.
	if succeeded := s.checkForUpdates(runningTime); !succeeded {
		return timeutil.Now().Add(updateCheckRetryFrequency)
	}

	// If we've just started up, we want to check again shortly after.
	// During startup is when a message is most likely to be actually seen by a
	// human operator so we check as early as possible, but this makes it hard to
	// differentiate real deployments vs short-lived instances for tests.
	if runningTime < updateCheckPostStartup {
		return timeutil.Now().Add(time.Hour - runningTime)
	}

	return timeutil.Now().Add(updateCheckFrequency)
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

// "diagnostics.reporting.enabled" enables reporting of metrics related to a
// node's storage (number, size and health of ranges) back to CockroachDB.
// Collecting this data from production clusters helps us understand and improve
// how our storage systems behave in real-world use cases.
//
// Note: while the setting itself is actually defined with a default value of
// `false`, it is usually automatically set to `true` when a cluster is created
// (or is migrated from a earlier beta version). This can be prevented with the
// env var COCKROACH_SKIP_ENABLING_DIAGNOSTIC_REPORTING.
//
// Doing this, rather than just using a default of `true`, means that a node
// will not errantly send a report using a default before loading settings.
var diagnosticsReportingEnabled = settings.RegisterBoolSetting(
	"diagnostics.reporting.enabled", "enable reporting diagnostic metrics to cockroach labs", false,
)

func (s *Server) maybeReportDiagnostics(scheduled time.Time, running time.Duration) time.Time {
	if scheduled.After(timeutil.Now()) {
		return scheduled
	}

	if diagnosticsReportingEnabled.Get() {
		s.reportDiagnostics()
	}

	return scheduled.Add(diagnosticReportFrequency)
}

func (s *Server) getReportingInfo() reportingInfo {
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
	return reportingInfo{summary, stores}
}

func (s *Server) reportDiagnostics() {
	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "usageReport")
	defer span.Finish()

	b := new(bytes.Buffer)
	if err := json.NewEncoder(b).Encode(s.getReportingInfo()); err != nil {
		log.Warning(ctx, err)
		return
	}

	q := reportingURL.Query()
	q.Set("version", build.GetInfo().Tag)
	q.Set("uuid", s.node.ClusterID.String())
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
