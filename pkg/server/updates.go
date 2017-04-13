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
	"github.com/cockroachdb/cockroach/pkg/keys"
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

const updateCheckFrequency = time.Hour * 24
const usageReportFrequency = updateCheckFrequency
const updateCheckPostStartup = time.Minute * 5
const updateCheckRetryFrequency = time.Hour
const updateMaxVersionsToReport = 3

const updateCheckJitterSeconds = 120

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
	s.stopper.RunWorker(func() {
		startup := timeutil.Now()
		nextUpdateCheck := time.Time{}

		var timer timeutil.Timer
		defer timer.Stop()
		for {
			runningTime := timeutil.Since(startup)

			nextUpdateCheck = s.maybeCheckForUpdates(nextUpdateCheck, runningTime)

			// Nominally we'll want to sleep until the next update check.
			wait := nextUpdateCheck.Sub(timeutil.Now())

			// If a usage report needs to happen sooner than the next update check,
			// we'll schedule a wake-up then instead.
			if reportWait := s.maybeReportUsage(runningTime); reportWait < wait {
				wait = reportWait
			}

			timer.Reset(addJitter(wait, updateCheckJitterSeconds))
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

// mayebReportUsage differs from maybeCheckForUpdates in that it persists the
// last-report time across restarts (since store usage, unlike version, isn't
// directly tied to the *running* binary).
func (s *Server) maybeReportUsage(running time.Duration) time.Duration {
	if running < updateCheckRetryFrequency {
		// On first check, we decline to report usage as metrics may not yet
		// be stable, so instead we request re-evaluation after a retry delay.
		return updateCheckRetryFrequency - running
	}
	if !settings.UsageReportingStoreStats() {
		return updateCheckFrequency
	}

	// Look up the persisted time to do the next report, if it exists.
	key := keys.NodeLastUsageReportKey(s.node.Descriptor.NodeID)
	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "usageReport")
	defer span.Finish()
	resp, err := s.db.Get(ctx, key)
	if err != nil {
		log.Infof(ctx, "error reading time of next usage report: %s", err)
		return updateCheckRetryFrequency
	}
	if resp.Exists() {
		whenToCheck, pErr := resp.Value.GetTime()
		if pErr != nil {
			log.Warningf(ctx, "error decoding time of next usage report: %s", err)
		} else if delay := whenToCheck.Sub(timeutil.Now()); delay > 0 {
			return delay
		}
	} else {
		log.Info(ctx, "no previously set usage report time.")
	}

	s.reportUsage(ctx)
	if err := s.db.Put(ctx, key, timeutil.Now().Add(usageReportFrequency)); err != nil {
		log.Infof(ctx, "error updating usage report time: %s", err)
	}
	return usageReportFrequency
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

func (s *Server) reportUsage(ctx context.Context) {
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
