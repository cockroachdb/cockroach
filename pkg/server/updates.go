// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"reflect"
	"runtime"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/cloudinfo"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/mitchellh/reflectwalk"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
)

const (
	updateCheckFrequency = time.Hour * 24
	// TODO(dt): switch to settings.
	updateCheckPostStartup    = time.Minute * 5
	updateCheckRetryFrequency = time.Hour
	updateMaxVersionsToReport = 3

	updateCheckJitterSeconds = 120
)

var diagnosticReportFrequency = settings.RegisterPublicNonNegativeDurationSetting(
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
func (s *Server) PeriodicallyCheckForUpdates(ctx context.Context) {
	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		defer log.RecoverAndReportNonfatalPanic(ctx, &s.st.SV)
		nextUpdateCheck := s.startTime
		nextDiagnosticReport := s.startTime

		var timer timeutil.Timer
		defer timer.Stop()
		for {
			now := timeutil.Now()
			runningTime := now.Sub(s.startTime)

			nextUpdateCheck = s.maybeCheckForUpdates(ctx, now, nextUpdateCheck, runningTime)
			nextDiagnosticReport = s.maybeReportDiagnostics(ctx, now, nextDiagnosticReport)

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
	ctx context.Context, now, scheduled time.Time, runningTime time.Duration,
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
	if succeeded := s.checkForUpdates(ctx); !succeeded {
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

func fillHardwareInfo(ctx context.Context, n *diagnosticspb.NodeInfo) {
	// Fill in hardware info (OS/CPU/Mem/etc).
	if platform, family, version, err := host.PlatformInformation(); err == nil {
		n.Os.Family = family
		n.Os.Platform = platform
		n.Os.Version = version
	}

	if virt, role, err := host.Virtualization(); err == nil && role == "guest" {
		n.Hardware.Virtualization = virt
	}

	if m, err := mem.VirtualMemory(); err == nil {
		n.Hardware.Mem.Available = m.Available
		n.Hardware.Mem.Total = m.Total
	}

	n.Hardware.Cpu.Numcpu = int32(runtime.NumCPU())
	if cpus, err := cpu.InfoWithContext(ctx); err == nil && len(cpus) > 0 {
		n.Hardware.Cpu.Sockets = int32(len(cpus))
		c := cpus[0]
		n.Hardware.Cpu.Cores = c.Cores
		n.Hardware.Cpu.Model = c.ModelName
		n.Hardware.Cpu.Mhz = float32(c.Mhz)
		n.Hardware.Cpu.Features = c.Flags
	}

	if l, err := load.AvgWithContext(ctx); err == nil {
		n.Hardware.Loadavg15 = float32(l.Load15)
	}

	n.Hardware.Provider, n.Hardware.InstanceClass = cloudinfo.GetInstanceClass(ctx)
	n.Topology.Provider, n.Topology.Region = cloudinfo.GetInstanceRegion(ctx)
}

// CheckForUpdates is part of the TestServerInterface.
func (s *Server) CheckForUpdates(ctx context.Context) {
	s.checkForUpdates(ctx)
}

// checkForUpdates calls home to check for new versions for the current platform
// and logs messages if it finds them, as well as if it encounters any errors.
// The returned boolean indicates if the check succeeded (and thus does not need
// to be re-attempted by the scheduler after a retry-interval).
func (s *Server) checkForUpdates(ctx context.Context) bool {
	ctx, span := s.AnnotateCtxWithSpan(ctx, "checkForUpdates")
	defer span.Finish()

	nodeInfo := s.collectNodeInfo(ctx)

	clusterInfo := diagnosticspb.ClusterInfo{
		ClusterID:  s.ClusterID(),
		IsInsecure: s.cfg.Insecure,
		IsInternal: sql.ClusterIsInternal(&s.st.SV),
	}
	var knobs *diagnosticspb.TestingKnobs
	if s.cfg.TestingKnobs.Server != nil {
		knobs = &s.cfg.TestingKnobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs
	}
	updatesURL := diagnosticspb.BuildUpdatesURL(&clusterInfo, &nodeInfo, knobs)
	if updatesURL == nil {
		return true // don't bother with asking for retry -- we'll never succeed.
	}

	res, err := httputil.Get(ctx, updatesURL.String())
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
		log.Warningf(ctx, "Error decoding updates info: %v", err)
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

func (s *Server) maybeReportDiagnostics(ctx context.Context, now, scheduled time.Time) time.Time {
	if scheduled.After(now) {
		return scheduled
	}

	// TODO(dt): we should allow tuning the reset and report intervals separately.
	// Consider something like rand.Float() > resetFreq/reportFreq here to sample
	// stat reset periods for reporting.
	if log.DiagnosticsReportingEnabled.Get(&s.st.SV) {
		s.ReportDiagnostics(ctx)
	}

	return scheduled.Add(diagnosticReportFrequency.Get(&s.st.SV))
}

func (s *Server) collectNodeInfo(ctx context.Context) diagnosticspb.NodeInfo {
	n := diagnosticspb.NodeInfo{
		NodeID: s.node.Descriptor.NodeID,
		Build:  build.GetInfo(),
		Uptime: int64(timeutil.Now().Sub(s.startTime).Seconds()),
	}

	licenseType, err := base.LicenseType(s.st)
	if err == nil {
		n.LicenseType = licenseType
	} else {
		log.Errorf(ctx, "error retrieving license type: %s", err)
	}

	fillHardwareInfo(ctx, &n)
	return n
}

func (s *Server) getReportingInfo(
	ctx context.Context, reset telemetry.ResetCounters,
) *diagnosticspb.DiagnosticReport {
	info := diagnosticspb.DiagnosticReport{}
	n := s.node.recorder.GenerateNodeStatus(ctx)
	info.Node = s.collectNodeInfo(ctx)

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
		info.Stores[i].Capacity = int64(r.Metrics["capacity"])
		info.Stores[i].Available = int64(r.Metrics["capacity.available"])
		info.Stores[i].Used = int64(r.Metrics["capacity.used"])
		info.Node.KeyCount += info.Stores[i].KeyCount
		info.Stores[i].RangeCount = int64(r.Metrics["replicas"])
		info.Node.RangeCount += info.Stores[i].RangeCount
		bytes := int64(r.Metrics["sysbytes"] + r.Metrics["intentbytes"] + r.Metrics["valbytes"] + r.Metrics["keybytes"])
		info.Stores[i].Bytes = bytes
		info.Node.Bytes += bytes
		info.Stores[i].EncryptionAlgorithm = int64(r.Metrics["rocksdb.encryption.algorithm"])
	}

	schema, err := s.collectSchemaInfo(ctx)
	if err != nil {
		log.Warningf(ctx, "error collecting schema info for diagnostic report: %+v", err)
		schema = nil
	}
	info.Schema = schema

	info.FeatureUsage = telemetry.GetFeatureCounts(telemetry.Quantized, reset)

	// Read the system.settings table to determine the settings for which we have
	// explicitly set values -- the in-memory SV has the set and default values
	// flattened for quick reads, but we'd rather only report the non-defaults.
	if datums, err := s.sqlServer.internalExecutor.QueryEx(
		ctx, "read-setting", nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"SELECT name FROM system.settings",
	); err != nil {
		log.Warningf(ctx, "failed to read settings: %s", err)
	} else {
		info.AlteredSettings = make(map[string]string, len(datums))
		for _, row := range datums {
			name := string(tree.MustBeDString(row[0]))
			info.AlteredSettings[name] = settings.RedactedValue(name, &s.st.SV)
		}
	}

	if datums, err := s.sqlServer.internalExecutor.QueryEx(
		ctx,
		"read-zone-configs",
		nil, /* txn */
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		"SELECT id, config FROM system.zones",
	); err != nil {
		log.Warningf(ctx, "%v", err)
	} else {
		info.ZoneConfigs = make(map[int64]zonepb.ZoneConfig)
		for _, row := range datums {
			id := int64(tree.MustBeDInt(row[0]))
			var zone zonepb.ZoneConfig
			if bytes, ok := row[1].(*tree.DBytes); !ok {
				continue
			} else {
				if err := protoutil.Unmarshal([]byte(*bytes), &zone); err != nil {
					log.Warningf(ctx, "unable to parse zone config %d: %v", id, err)
					continue
				}
			}
			var anonymizedZone zonepb.ZoneConfig
			anonymizeZoneConfig(&anonymizedZone, zone, secret)
			info.ZoneConfigs[id] = anonymizedZone
		}
	}

	info.SqlStats = s.sqlServer.pgServer.SQLServer.GetScrubbedReportingStats()
	return &info
}

func anonymizeZoneConfig(dst *zonepb.ZoneConfig, src zonepb.ZoneConfig, secret string) {
	if src.RangeMinBytes != nil {
		dst.RangeMinBytes = proto.Int64(*src.RangeMinBytes)
	}
	if src.RangeMaxBytes != nil {
		dst.RangeMaxBytes = proto.Int64(*src.RangeMaxBytes)
	}
	if src.GC != nil {
		dst.GC = &zonepb.GCPolicy{TTLSeconds: src.GC.TTLSeconds}
	}
	if src.NumReplicas != nil {
		dst.NumReplicas = proto.Int32(*src.NumReplicas)
	}
	dst.Constraints = make([]zonepb.ConstraintsConjunction, len(src.Constraints))
	for i := range src.Constraints {
		dst.Constraints[i].NumReplicas = src.Constraints[i].NumReplicas
		dst.Constraints[i].Constraints = make([]zonepb.Constraint, len(src.Constraints[i].Constraints))
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
	dst.LeasePreferences = make([]zonepb.LeasePreference, len(src.LeasePreferences))
	for i := range src.LeasePreferences {
		dst.LeasePreferences[i].Constraints = make([]zonepb.Constraint, len(src.LeasePreferences[i].Constraints))
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
	dst.Subzones = make([]zonepb.Subzone, len(src.Subzones))
	for i := range src.Subzones {
		dst.Subzones[i].IndexID = src.Subzones[i].IndexID
		dst.Subzones[i].PartitionName = sql.HashForReporting(secret, src.Subzones[i].PartitionName)
		anonymizeZoneConfig(&dst.Subzones[i].Config, src.Subzones[i].Config, secret)
	}
}

// ReportDiagnostics is part of the TestServerInterface.
func (s *Server) ReportDiagnostics(ctx context.Context) {
	ctx, span := s.AnnotateCtxWithSpan(ctx, "usageReport")
	defer span.Finish()

	report := s.getReportingInfo(ctx, telemetry.ResetCounts)

	clusterInfo := diagnosticspb.ClusterInfo{
		ClusterID:  s.ClusterID(),
		IsInsecure: s.cfg.Insecure,
		IsInternal: sql.ClusterIsInternal(&s.st.SV),
	}
	var knobs *diagnosticspb.TestingKnobs
	if s.cfg.TestingKnobs.Server != nil {
		knobs = &s.cfg.TestingKnobs.Server.(*TestingKnobs).DiagnosticsTestingKnobs
	}
	reportingURL := diagnosticspb.BuildReportingURL(&clusterInfo, &report.Node, knobs)
	if reportingURL == nil {
		return
	}

	b, err := protoutil.Marshal(report)
	if err != nil {
		log.Warningf(ctx, "%v", err)
		return
	}

	res, err := httputil.Post(
		ctx, reportingURL.String(), "application/x-protobuf", bytes.NewReader(b),
	)
	if err != nil {
		if log.V(2) {
			// This is probably going to be relatively common in production
			// environments where network access is usually curtailed.
			log.Warningf(ctx, "failed to report node usage metrics: %v", err)
		}
		return
	}
	defer res.Body.Close()
	b, err = ioutil.ReadAll(res.Body)
	if err != nil || res.StatusCode != http.StatusOK {
		log.Warningf(ctx, "failed to report node usage metrics: status: %s, body: %s, "+
			"error: %v", res.Status, b, err)
		return
	}
	s.sqlServer.pgServer.SQLServer.ResetReportedStats(ctx)
}

func (s *Server) collectSchemaInfo(ctx context.Context) ([]sqlbase.TableDescriptor, error) {
	startKey := keys.TODOSQLCodec.TablePrefix(keys.DescriptorTableID)
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
		if t := desc.Table(kv.Value.Timestamp); t != nil && t.ID > keys.MaxReservedDescID {
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
