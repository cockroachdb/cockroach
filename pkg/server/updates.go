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
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const baseUpdatesURL = `https://register.cockroachdb.com/api/clusters/updates`
const baseReportingURL = `https://register.cockroachdb.com/api/report`

var updatesURL, reportingURL *url.URL

func init() {
	var err error
	updatesURL, err = url.Parse(baseUpdatesURL)
	if err != nil {
		panic(err)
	}
	reportingURL, err = url.Parse(baseReportingURL)
	if err != nil {
		panic(err)
	}
}

const updateCheckFrequency = time.Hour * 24
const updateCheckJitterSeconds = 120
const updateCheckRetryFrequency = time.Hour

const optinKey = serverUIDataKeyPrefix + "optin-reporting"

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

		for {
			// `maybeCheckForUpdates` and `maybeReportUsage` both return the
			// duration until they should next be checked.
			// Wait for the shorter of the durations returned by the two checks.
			wait := s.maybeCheckForUpdates()
			if reportWait := s.maybeReportUsage(timeutil.Since(startup)); reportWait < wait {
				wait = reportWait
			}
			jitter := rand.Intn(updateCheckJitterSeconds) - updateCheckJitterSeconds/2
			wait = wait + (time.Duration(jitter) * time.Second)
			select {
			case <-s.stopper.ShouldQuiesce():
				return
			case <-time.After(wait):
			}
		}
	})
}

// Determines if it is time to check for updates and does so if it is.
// Returns a duration indicating when to make the next call to this method.
func (s *Server) maybeCheckForUpdates() time.Duration {
	return s.maybeRunPeriodicCheck("updates check", keys.UpdateCheckCluster, s.checkForUpdates)
}

// If the time is greater than the timestamp stored at `key`, run `f`.
// Before running `f`, the timestamp is updated forward by a small amount via
// a compare-and-swap to ensure at-most-one concurrent execution. After `f`
// executes the timestamp is set to the next execution time.
// Returns how long until `f` should be run next (i.e. when this method should
// be called again).
func (s *Server) maybeRunPeriodicCheck(
	op string, key roachpb.Key, f func(context.Context),
) time.Duration {
	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "op")
	defer span.Finish()

	// Add the op name to the log context.
	ctx = log.WithLogTag(ctx, op, nil)

	resp, err := s.db.Get(ctx, key)
	if err != nil {
		log.Infof(ctx, "error reading time: %s", err)
		return updateCheckRetryFrequency
	}

	// We should early returned below if either the next check time is in the
	// future or if the atomic compare-and-set of that time failed (which
	// would happen if two nodes tried at the same time).
	if resp.Exists() {
		whenToCheck, pErr := resp.Value.GetTime()
		if pErr != nil {
			log.Warningf(ctx, "error decoding time: %s", err)
			return updateCheckRetryFrequency
		} else if delay := whenToCheck.Sub(timeutil.Now()); delay > 0 {
			return delay
		}

		nextRetry := whenToCheck.Add(updateCheckRetryFrequency)
		if err := s.db.CPut(ctx, key, nextRetry, whenToCheck); err != nil {
			if log.V(2) {
				log.Infof(ctx, "could not set next version check time (maybe another node checked?): %s", err)
			}
			return updateCheckRetryFrequency
		}
	} else {
		log.Infof(ctx, "No previous %s time.", op)
		nextRetry := timeutil.Now().Add(updateCheckRetryFrequency)
		// CPut with `nil` prev value to assert that no other node has checked.
		if err := s.db.CPut(ctx, key, nextRetry, nil); err != nil {
			if log.V(2) {
				log.Infof(ctx, "Could not set %s time (maybe another node checked?): %v", op, err)
			}
			return updateCheckRetryFrequency
		}
	}

	f(ctx)

	if err := s.db.Put(ctx, key, timeutil.Now().Add(updateCheckFrequency)); err != nil {
		log.Infof(ctx, "Error updating %s time: %v", op, err)
	}
	return updateCheckFrequency
}

func (s *Server) checkForUpdates(ctx context.Context) {
	q := updatesURL.Query()
	q.Set("version", build.GetInfo().Tag)
	q.Set("uuid", s.node.ClusterID.String())
	updatesURL.RawQuery = q.Encode()

	res, err := http.Get(updatesURL.String())
	if err != nil {
		// This is probably going to be relatively common in production
		// environments where network access is usually curtailed.
		if log.V(2) {
			log.Warning(ctx, "Failed to check for updates: ", err)
		}
		return
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(res.Body)
		log.Warningf(ctx, "Failed to check for updates: status: %s, body: %s, error: %v",
			res.Status, b, err)
		return
	}

	decoder := json.NewDecoder(res.Body)
	r := struct {
		Details []versionInfo `json:"details"`
	}{}

	err = decoder.Decode(&r)
	if err != nil && err != io.EOF {
		log.Warning(ctx, "Error decoding updates info: ", err)
		return
	}

	for _, v := range r.Details {
		log.Infof(ctx, "A new version is available: %s, details: %s", v.Version, v.Details)
	}
}

func (s *Server) usageReportingEnabled() bool {
	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "usage-reporting")
	defer span.Finish()

	// Grab the optin value from the database.
	req := &serverpb.GetUIDataRequest{Keys: []string{optinKey}}
	resp, err := s.admin.GetUIData(ctx, req)
	if err != nil {
		log.Warning(ctx, err)
		return false
	}

	val, ok := resp.KeyValues[optinKey]
	if !ok {
		// Key wasn't found, so we opt out by default.
		return false
	}
	optin, err := strconv.ParseBool(string(val.Value))
	if err != nil {
		log.Warningf(ctx, "could not parse optin value (%q): %v", val.Value, err)
		return false
	}
	return optin
}

func (s *Server) maybeReportUsage(running time.Duration) time.Duration {
	if running < updateCheckRetryFrequency {
		// On first check, we decline to report usage as metrics may not yet
		// be stable, so instead we request re-evaluation after a retry delay.
		return updateCheckRetryFrequency - running
	}
	if !s.usageReportingEnabled() {
		return updateCheckFrequency
	}
	return s.maybeRunPeriodicCheck("metrics reporting",
		keys.NodeLastUsageReportKey(s.node.Descriptor.NodeID), s.reportUsage)
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
	if err != nil && log.V(2) {
		// This is probably going to be relatively common in production
		// environments where network access is usually curtailed.
		log.Warning(ctx, "Failed to report node usage metrics: ", err)
		return
	}

	if res.StatusCode != http.StatusOK {
		b, err := ioutil.ReadAll(res.Body)
		log.Warningf(ctx, "Failed to report node usage metrics: status: %s, body: %s, "+
			"error: %v", res.Status, b, err)
	}
}
