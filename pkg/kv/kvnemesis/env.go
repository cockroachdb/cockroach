// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	gosql "database/sql"
	"fmt"
	"regexp"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	kvpb "github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// Logger is the log sink used by kvnemesis.
type Logger interface {
	Helper()
	Logf(string, ...interface{})
	WriteFile(basename string, contents string) string
}

// Env manipulates the environment (cluster settings, zone configurations) that
// the Applier operates in.
type Env struct {
	SQLDBs  []*gosql.DB
	Tracker *SeqTracker
	L       Logger
}

func (e *Env) anyNode() *gosql.DB {
	// NOTE: There is currently no need to round-robin through the sql gateways,
	// so we always just return the first DB.
	return e.SQLDBs[0]
}

// CheckConsistency runs a consistency check on all ranges in the given span,
// primarily to verify that MVCC stats are accurate. Any failures are returned
// as a list of errors. RANGE_CONSISTENT_STATS_ESTIMATED is considered a
// success, since stats estimates are fine (if unfortunate).
func (e *Env) CheckConsistency(ctx context.Context, span roachpb.Span) []error {
	rows, err := e.anyNode().QueryContext(ctx, fmt.Sprintf(`
		SELECT range_id, start_key_pretty, status, detail
		FROM crdb_internal.check_consistency(false, b'\x%x', b'\x%x')
		ORDER BY range_id ASC`,
		span.Key, span.EndKey,
	))
	if err != nil {
		return []error{err}
	}
	defer rows.Close()

	var failures []error
	for rows.Next() {
		var rangeID int
		var key, status, detail string
		if err := rows.Scan(&rangeID, &key, &status, &detail); err != nil {
			return []error{err}
		}
		// NB: There's a known issue that can result in a 10-byte discrepancy in
		// SysBytes. See:
		// https://github.com/cockroachdb/cockroach/issues/93896
		//
		// This isn't critical, so we ignore such discrepancies.
		if status == kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_INCORRECT.String() {
			m := regexp.MustCompile(`.*\ndelta \(stats-computed\): \{(.*)\}`).FindStringSubmatch(detail)
			if len(m) > 1 {
				delta := m[1]
				// Strip out LastUpdateNanos and all zero-valued fields.
				delta = regexp.MustCompile(`LastUpdateNanos:\d+`).ReplaceAllString(delta, "")
				delta = regexp.MustCompile(`\S+:0\b`).ReplaceAllString(delta, "")
				if regexp.MustCompile(`^\s*SysBytes:-?10\s*$`).MatchString(delta) {
					continue
				}
			}
		}
		switch status {
		case kvpb.CheckConsistencyResponse_RANGE_INDETERMINATE.String():
			// Can't do anything, so let it slide.
		case kvpb.CheckConsistencyResponse_RANGE_CONSISTENT.String():
			// Good.
		case kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_ESTIMATED.String():
			// Ok.
		default:
			failures = append(failures, errors.Errorf("range %d (%s) %s:\n%s", rangeID, key, status, detail))
		}
	}
	return failures
}

// SetClosedTimestampInterval sets the kv.closed_timestamp.target_duration
// cluster setting to the provided duration.
func (e *Env) SetClosedTimestampInterval(ctx context.Context, d time.Duration) error {
	return e.SetClusterSetting(ctx, "kv.closed_timestamp.target_duration", d.String())
}

// ResetClosedTimestampInterval resets the kv.closed_timestamp.target_duration
// cluster setting to its default value.
func (e *Env) ResetClosedTimestampInterval(ctx context.Context) error {
	return e.SetClusterSettingToDefault(ctx, "kv.closed_timestamp.target_duration")
}

// SetClusterSetting sets the cluster setting with the provided name to the
// provided value.
func (e *Env) SetClusterSetting(ctx context.Context, name, val string) error {
	q := fmt.Sprintf(`SET CLUSTER SETTING %s = '%s'`, name, val)
	_, err := e.anyNode().ExecContext(ctx, q)
	return err
}

// SetClusterSettingToDefault resets the cluster setting with the provided name
// to its default value.
func (e *Env) SetClusterSettingToDefault(ctx context.Context, name string) error {
	q := fmt.Sprintf(`SET CLUSTER SETTING %s TO DEFAULT`, name)
	_, err := e.anyNode().ExecContext(ctx, q)
	return err
}

// UpdateZoneConfig updates the zone configuration with the provided ID using
// the provided function. If no such zone exists, a new one is created.
func (e *Env) UpdateZoneConfig(
	ctx context.Context, zoneID int, updateZone func(*zonepb.ZoneConfig),
) error {
	return crdb.ExecuteTx(ctx, e.anyNode(), nil, func(tx *gosql.Tx) error {
		// Read existing zone configuration.
		var zone zonepb.ZoneConfig
		var zoneRaw []byte
		const q1 = `SELECT config FROM system.zones WHERE id = $1`
		if err := tx.QueryRowContext(ctx, q1, zoneID).Scan(&zoneRaw); err != nil {
			if !errors.Is(err, gosql.ErrNoRows) {
				return err
			}
			// Zone does not exist. Create it.
			zone = zonepb.DefaultZoneConfig()
		} else {
			if err := protoutil.Unmarshal(zoneRaw, &zone); err != nil {
				return errors.Wrap(err, "unmarshaling existing zone")
			}
		}

		// Update zone configuration proto.
		updateZone(&zone)
		zoneRaw, err := protoutil.Marshal(&zone)
		if err != nil {
			return errors.Wrap(err, "marshaling new zone")
		}

		// Rewrite updated zone configuration.
		const q2 = `UPSERT INTO system.zones (id, config) VALUES ($1, $2)`
		_, err = tx.ExecContext(ctx, q2, zoneID, zoneRaw)
		return err
	})
}
