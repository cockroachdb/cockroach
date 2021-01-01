// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package diagnostics

import (
	"bytes"
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/cloudinfo"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/mitchellh/reflectwalk"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/host"
	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/mem"
	"google.golang.org/protobuf/proto"
)

var ReportFrequency = settings.RegisterDurationSetting(
	"diagnostics.reporting.interval",
	"interval at which diagnostics data should be reported",
	time.Hour,
	settings.NonNegativeDuration,
).WithPublic()

const updateCheckJitterSeconds = 120

type Reporter struct {
	StartTime  time.Time
	AmbientCtx *log.AmbientContext
	Config     *base.Config
	Settings   *cluster.Settings

	// ClusterID is not yet available at the time the reporter is created, so
	// instead initialize with a function that is guaranteed to
	ClusterID    func() uuid.UUID
	TenantID     roachpb.TenantID
	SqlServer    *sql.Server
	InternalExec *sql.InternalExecutor
	DB           *kv.DB
	Recorder     *status.MetricsRecorder

	// Locality is a description of the topography of the server.
	Locality roachpb.Locality

	// TestingKnobs is used for internal test controls only.
	TestingKnobs *diagnosticspb.TestingKnobs
}

func (r *Reporter) PeriodicallyReportDiagnostics(ctx context.Context, stopper *stop.Stopper) {
	stopper.RunWorker(ctx, func(ctx context.Context) {
		defer logcrash.RecoverAndReportNonfatalPanic(ctx, &r.Settings.SV)
		nextReport := r.StartTime

		var timer timeutil.Timer
		defer timer.Stop()
		for {
			// TODO(dt): we should allow tuning the reset and report intervals separately.
			// Consider something like rand.Float() > resetFreq/reportFreq here to sample
			// stat reset periods for reporting.
			if logcrash.DiagnosticsReportingEnabled.Get(&r.Settings.SV) {
				r.ReportDiagnostics(ctx)
			}

			nextReport = nextReport.Add(ReportFrequency.Get(&r.Settings.SV))

			timer.Reset(addJitter(nextReport.Sub(timeutil.Now()), updateCheckJitterSeconds))
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-timer.C:
				timer.Read = true
			}
		}
	})
}

func (r *Reporter) ReportDiagnostics(ctx context.Context) {
	ctx, span := r.AmbientCtx.AnnotateCtxWithSpan(ctx, "usageReport")
	defer span.Finish()

	report := r.getReportingInfo(ctx, telemetry.ResetCounts)

	clusterInfo := diagnosticspb.ClusterInfo{
		ClusterID:  r.ClusterID(),
		IsInsecure: r.Config.Insecure,
		IsInternal: sql.ClusterIsInternal(&r.Settings.SV),
	}
	reportingURL := diagnosticspb.BuildReportingURL(&clusterInfo, &report.Node, &report.Tenant, r.TestingKnobs)
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
	r.SqlServer.ResetReportedStats(ctx)
}

func (r *Reporter) getReportingInfo(
	ctx context.Context, reset telemetry.ResetCounters,
) *diagnosticspb.DiagnosticReport {
	info := diagnosticspb.DiagnosticReport{}
	secret := sql.ClusterSecret.Get(&r.Settings.SV)

	// Do not collect node or store information for tenant reports.
	if r.TenantID == roachpb.SystemTenantID {
		n := r.Recorder.GenerateNodeStatus(ctx)
		info.Node = r.collectNodeInfo(ctx, n.Desc.NodeID)

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

		// Add in the localities.
		for _, tier := range r.Locality.Tiers {
			info.Node.Locality.Tiers = append(info.Node.Locality.Tiers, roachpb.Tier{
				Key:   sql.HashForReporting(secret, tier.Key),
				Value: sql.HashForReporting(secret, tier.Value),
			})
		}
	} else {
		info.Tenant = r.collectTenantInfo(ctx, r.TenantID)
	}

	schema, err := r.collectSchemaInfo(ctx)
	if err != nil {
		log.Warningf(ctx, "error collecting schema info for diagnostic report: %+v", err)
		schema = nil
	}
	info.Schema = schema

	info.FeatureUsage = telemetry.GetFeatureCounts(telemetry.Quantized, reset)

	// Read the system.settings table to determine the settings for which we have
	// explicitly set values -- the in-memory SV has the set and default values
	// flattened for quick reads, but we'd rather only report the non-defaults.
	if datums, err := r.InternalExec.QueryEx(
		ctx, "read-setting", nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
		"SELECT name FROM system.settings",
	); err != nil {
		log.Warningf(ctx, "failed to read settings: %s", err)
	} else {
		info.AlteredSettings = make(map[string]string, len(datums))
		for _, row := range datums {
			name := string(tree.MustBeDString(row[0]))
			info.AlteredSettings[name] = settings.RedactedValue(name, &r.Settings.SV)
		}
	}

	if datums, err := r.InternalExec.QueryEx(
		ctx,
		"read-zone-configs",
		nil, /* txn */
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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

	info.SqlStats = r.SqlServer.GetScrubbedReportingStats()
	return &info
}

func (r *Reporter) collectNodeInfo(
	ctx context.Context, nodeID roachpb.NodeID,
) diagnosticspb.NodeInfo {
	n := diagnosticspb.NodeInfo{
		NodeID:      nodeID,
		Build:       build.GetInfo(),
		Uptime:      int64(timeutil.Now().Sub(r.StartTime).Seconds()),
		LicenseType: getLicenseType(ctx, r.Settings),
	}

	fillHardwareInfo(ctx, &n)
	return n
}

func (r *Reporter) collectTenantInfo(
	ctx context.Context, tenantID roachpb.TenantID,
) diagnosticspb.TenantInfo {
	return diagnosticspb.TenantInfo{
		TenantID:    tenantID.ToUint64(),
		Build:       build.GetInfo(),
		Uptime:      int64(timeutil.Now().Sub(r.StartTime).Seconds()),
		LicenseType: getLicenseType(ctx, r.Settings),
	}
}

func (r *Reporter) collectSchemaInfo(ctx context.Context) ([]descpb.TableDescriptor, error) {
	startKey := keys.MakeSQLCodec(r.TenantID).TablePrefix(keys.DescriptorTableID)
	endKey := startKey.PrefixEnd()
	kvs, err := r.DB.Scan(ctx, startKey, endKey, 0)
	if err != nil {
		return nil, err
	}
	tables := make([]descpb.TableDescriptor, 0, len(kvs))
	redactor := stringRedactor{}
	for _, kv := range kvs {
		var desc descpb.Descriptor
		if err := kv.ValueProto(&desc); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal SQL descriptor", kv.Key)
		}
		if t := descpb.TableFromDescriptor(&desc, kv.Value.Timestamp); t != nil && t.ID > keys.MaxReservedDescID {
			if err := reflectwalk.Walk(t, redactor); err != nil {
				panic(err) // stringRedactor never returns a non-nil err
			}
			tables = append(tables, *t)
		}
	}
	return tables, nil
}

func getLicenseType(ctx context.Context, settings *cluster.Settings) string {
	licenseType, err := base.LicenseType(settings)
	if err != nil {
		log.Errorf(ctx, "error retrieving license type: %s", err)
		return ""
	}
	return licenseType
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

type stringRedactor struct{}

func (stringRedactor) Primitive(v reflect.Value) error {
	if v.Kind() == reflect.String && v.String() != "" {
		v.Set(reflect.ValueOf("_").Convert(v.Type()))
	}
	return nil
}

// randomly shift `d` to be up to `jitterSec` shorter or longer.
func addJitter(d time.Duration, jitterSec int) time.Duration {
	j := time.Duration(rand.Intn(jitterSec*2)-jitterSec) * time.Second
	return d + j
}
