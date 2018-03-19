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
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/mitchellh/reflectwalk"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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

var envChannel = envutil.EnvOrDefaultString("COCKROACH_CHANNEL", "unknown")

const (
	updateCheckFrequency = time.Hour * 24
	// TODO(dt): switch to settings.
	updateCheckPostStartup    = time.Minute * 5
	updateCheckRetryFrequency = time.Hour
	updateMaxVersionsToReport = 3

	updateCheckJitterSeconds = 120
)

var diagnosticReportFrequency = settings.RegisterNonNegativeDurationSetting(
	"diagnostics.reporting.interval",
	"interval at which diagnostics data should be reported",
	time.Hour,
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

	// if diagnostics reporting is disabled, we should assume that means that the
	// user doesn't want us phoning home for new-version checks either.
	if !log.DiagnosticsReportingEnabled.Get(&s.st.SV) {
		return now.Add(updateCheckFrequency)
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

func addInfoToURL(ctx context.Context, url *url.URL, s *Server, runningTime time.Duration) {
	q := url.Query()
	b := build.GetInfo()
	q.Set("version", b.Tag)
	q.Set("platform", b.Platform)
	q.Set("uuid", s.ClusterID().String())
	q.Set("nodeid", s.NodeID().String())
	q.Set("uptime", strconv.Itoa(int(runningTime.Seconds())))
	q.Set("insecure", strconv.FormatBool(s.cfg.Insecure))
	q.Set("internal",
		strconv.FormatBool(strings.Contains(sql.ClusterOrganization.Get(&s.st.SV), "Cockroach Labs")))
	q.Set("buildchannel", b.Channel)
	q.Set("envchannel", envChannel)
	licenseType, err := base.LicenseType(s.st)
	if err == nil {
		q.Set("licensetype", licenseType)
	} else {
		log.Errorf(ctx, "error retrieving license type: %s", err)
	}
	url.RawQuery = q.Encode()
}

// checkForUpdates calls home to check for new versions for the current platform
// and logs messages if it finds them, as well as if it encounters any errors.
// The returned boolean indicates if the check succeeded (and thus does not need
// to be re-attempted by the scheduler after a retry-interval).
func (s *Server) checkForUpdates(runningTime time.Duration) bool {
	if updatesURL == nil {
		return true // don't bother with asking for retry -- we'll never succeed.
	}
	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "checkForUpdates")
	defer span.Finish()

	addInfoToURL(ctx, updatesURL, s, runningTime)

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

func (s *Server) maybeReportDiagnostics(
	ctx context.Context, now, scheduled time.Time, running time.Duration,
) time.Time {
	if scheduled.After(now) {
		return scheduled
	}

	// TODO(dt): we should allow tuning the reset and report intervals separately.
	// Consider something like rand.Float() > resetFreq/reportFreq here to sample
	// stat reset periods for reporting.
	if log.DiagnosticsReportingEnabled.Get(&s.st.SV) {
		s.reportDiagnostics(running)
	}
	if !s.cfg.UseLegacyConnHandling {
		s.pgServer.SQLServer.ResetStatementStats(ctx)
		s.pgServer.SQLServer.ResetErrorCounts()
	} else {
		s.sqlExecutor.ResetStatementStats(ctx)
		s.sqlExecutor.ResetErrorCounts()
	}

	return scheduled.Add(diagnosticReportFrequency.Get(&s.st.SV))
}

func (s *Server) getReportingInfo(ctx context.Context) *diagnosticspb.DiagnosticReport {
	info := diagnosticspb.DiagnosticReport{}
	n := s.node.recorder.GetStatusSummary(ctx)
	info.Node = diagnosticspb.NodeInfo{NodeID: s.node.Descriptor.NodeID}

	secret := sql.ClusterSecret.Get(&s.cfg.Settings.SV)
	// Add in the localities.
	for _, tier := range s.node.Descriptor.Locality.Tiers {
		info.Node.Locality.Tiers = append(info.Node.Locality.Tiers, roachpb.Tier{
			Key:   sql.HashForReporting(secret, tier.Key),
			Value: sql.HashForReporting(secret, tier.Value),
		})
	}

	info.Stores = make([]diagnosticspb.StoreInfo, len(n.StoreStatuses))
	for i, r := range n.StoreStatuses {
		info.Stores[i].NodeID = r.Desc.Node.NodeID
		info.Stores[i].StoreID = r.Desc.StoreID
		info.Stores[i].KeyCount = int64(r.Metrics["keycount"])
		info.Node.KeyCount += info.Stores[i].KeyCount
		info.Stores[i].RangeCount = int64(r.Metrics["replicas"])
		info.Node.RangeCount += info.Stores[i].RangeCount
		bytes := int64(r.Metrics["sysbytes"] + r.Metrics["intentbytes"] + r.Metrics["valbytes"] + r.Metrics["keybytes"])
		info.Stores[i].Bytes = bytes
		info.Node.Bytes += bytes
	}

	schema, err := s.collectSchemaInfo(ctx)
	if err != nil {
		log.Warningf(ctx, "error collecting schema info for diagnostic report: %+v", err)
		schema = nil
	}
	info.Schema = schema
	info.ErrorCounts = make(map[string]int64)
	info.UnimplementedErrors = make(map[string]int64)

	// Read the system.settings table to determine the settings for which we have
	// explicitly set values -- the in-memory SV has the set and default values
	// flattened for quick reads, but we'd rather only report the non-defaults.
	if datums, _, err := (&sql.InternalExecutor{ExecCfg: s.execCfg}).QueryRows(
		ctx, "read-setting", "SELECT name FROM system.settings",
	); err != nil {
		log.Warning(ctx, err)
	} else {
		info.AlteredSettings = make(map[string]string, len(datums))
		for _, row := range datums {
			name := string(tree.MustBeDString(row[0]))
			info.AlteredSettings[name] = settings.SanitizedValue(name, &s.st.SV)
		}
	}

	if datums, _, err := (&sql.InternalExecutor{ExecCfg: s.execCfg}).QueryRows(
		ctx,
		"read-zone-configs",
		"SELECT id, config FROM system.zones",
	); err != nil {
		log.Warning(ctx, err)
	} else {
		info.ZoneConfigs = make(map[int64]config.ZoneConfig)
		for _, row := range datums {
			id := int64(tree.MustBeDInt(row[0]))
			configProto := []byte(*(row[1].(*tree.DBytes)))
			var zone config.ZoneConfig
			if err := protoutil.Unmarshal(configProto, &zone); err != nil {
				log.Warningf(ctx, "unable to parse zone config %d: %v", id, err)
				continue
			}
			var anonymizedZone config.ZoneConfig
			anonymizeZoneConfig(&anonymizedZone, zone, secret)
			info.ZoneConfigs[id] = anonymizedZone
		}
	}

	if !s.cfg.UseLegacyConnHandling {
		info.SqlStats = s.pgServer.SQLServer.GetScrubbedStmtStats()
		s.pgServer.SQLServer.FillErrorCounts(info.ErrorCounts, info.UnimplementedErrors)
	} else {
		info.SqlStats = s.sqlExecutor.GetScrubbedStmtStats()
		s.sqlExecutor.FillErrorCounts(info.ErrorCounts, info.UnimplementedErrors)
	}
	return &info
}

func anonymizeZoneConfig(dst *config.ZoneConfig, src config.ZoneConfig, secret string) {
	dst.RangeMinBytes = src.RangeMinBytes
	dst.RangeMaxBytes = src.RangeMaxBytes
	dst.GC.TTLSeconds = src.GC.TTLSeconds
	dst.NumReplicas = src.NumReplicas
	dst.Constraints = make([]config.Constraints, len(src.Constraints))
	for i := range src.Constraints {
		dst.Constraints[i].NumReplicas = src.Constraints[i].NumReplicas
		dst.Constraints[i].Constraints = make([]config.Constraint, len(src.Constraints[i].Constraints))
		for j := range src.Constraints[i].Constraints {
			dst.Constraints[i].Constraints[j].Type = src.Constraints[i].Constraints[j].Type
			if key := src.Constraints[i].Constraints[j].Key; key != "" {
				dst.Constraints[i].Constraints[j].Key = sql.HashForReporting(secret, key)
			}
			if val := src.Constraints[i].Constraints[j].Value; val != "" {
				dst.Constraints[i].Constraints[j].Value = sql.HashForReporting(secret, val)
			}
		}
	}
	dst.LeasePreferences = make([]config.LeasePreference, len(src.LeasePreferences))
	for i := range src.LeasePreferences {
		dst.LeasePreferences[i].Constraints = make([]config.Constraint, len(src.LeasePreferences[i].Constraints))
		for j := range src.LeasePreferences[i].Constraints {
			dst.LeasePreferences[i].Constraints[j].Type = src.LeasePreferences[i].Constraints[j].Type
			if key := src.LeasePreferences[i].Constraints[j].Key; key != "" {
				dst.LeasePreferences[i].Constraints[j].Key = sql.HashForReporting(secret, key)
			}
			if val := src.LeasePreferences[i].Constraints[j].Value; val != "" {
				dst.LeasePreferences[i].Constraints[j].Value = sql.HashForReporting(secret, val)
			}
		}
	}
	dst.Subzones = make([]config.Subzone, len(src.Subzones))
	for i := range src.Subzones {
		dst.Subzones[i].IndexID = src.Subzones[i].IndexID
		dst.Subzones[i].PartitionName = sql.HashForReporting(secret, src.Subzones[i].PartitionName)
		anonymizeZoneConfig(&dst.Subzones[i].Config, src.Subzones[i].Config, secret)
	}
}

func (s *Server) reportDiagnostics(runningTime time.Duration) {
	if reportingURL == nil {
		return
	}
	ctx, span := s.AnnotateCtxWithSpan(context.Background(), "usageReport")
	defer span.Finish()

	b, err := protoutil.Marshal(s.getReportingInfo(ctx))
	if err != nil {
		log.Warning(ctx, err)
		return
	}

	addInfoToURL(ctx, reportingURL, s, runningTime)
	res, err := http.Post(reportingURL.String(), "application/x-protobuf", bytes.NewReader(b))
	if err != nil {
		if log.V(2) {
			// This is probably going to be relatively common in production
			// environments where network access is usually curtailed.
			log.Warning(ctx, "failed to report node usage metrics: ", err)
		}
		return
	}
	defer res.Body.Close()

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
