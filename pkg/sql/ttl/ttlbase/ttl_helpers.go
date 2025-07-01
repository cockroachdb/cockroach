// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttlbase

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/spanutils"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

const (
	// DefaultAOSTDuration is the default duration to use in the AS OF SYSTEM TIME
	// clause used in the SELECT query.
	DefaultAOSTDuration         = -time.Second * 30
	DefaultSelectBatchSizeValue = 500
)

var (
	defaultSelectBatchSize = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"sql.ttl.default_select_batch_size",
		"default amount of rows to select in a single query during a TTL job",
		DefaultSelectBatchSizeValue,
		settings.PositiveInt,
		settings.WithPublic,
	)
	defaultDeleteBatchSize = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"sql.ttl.default_delete_batch_size",
		"default amount of rows to delete in a single query during a TTL job",
		100,
		settings.PositiveInt,
		settings.WithPublic,
	)
	defaultSelectRateLimit = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"sql.ttl.default_select_rate_limit",
		"default select rate limit (rows per second) per node for each TTL job. Use 0 to signify no rate limit.",
		0,
		settings.NonNegativeInt,
		settings.WithPublic,
	)
	defaultDeleteRateLimit = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"sql.ttl.default_delete_rate_limit",
		"default delete rate limit (rows per second) per node for each TTL job. Use 0 to signify no rate limit.",
		100,
		settings.NonNegativeInt,
		settings.WithPublic,
	)
	jobEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"sql.ttl.job.enabled",
		"whether the TTL job is enabled",
		true,
		settings.WithPublic,
	)
	changefeedReplicationDisabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"sql.ttl.changefeed_replication.disabled",
		"if true, deletes issued by TTL will not be replicated via changefeeds "+
			"(this setting will be ignored by changefeeds that have the ignore_disable_changefeed_replication option set; "+
			"such changefeeds will continue to replicate all TTL deletes)",
		false,
		settings.WithPublic,
	)
	processorConcurrencyOverride = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"sql.ttl.processor_concurrency",
		"override for the TTL job processor concurrency (0 means use default based on GOMAXPROCS, "+
			"and any value greater than GOMAXPROCS will be capped at GOMAXPROCS)",
		0,
		settings.NonNegativeInt,
	)
)

// GetSelectBatchSize returns the table storage param value if specified or
// falls back to the cluster setting.
func GetSelectBatchSize(sv *settings.Values, ttl *catpb.RowLevelTTL) int64 {
	bs := ttl.SelectBatchSize
	if bs == 0 {
		bs = defaultSelectBatchSize.Get(sv)
	}
	return bs
}

// GetDeleteBatchSize returns the table storage param value if specified or
// falls back to the cluster setting.
func GetDeleteBatchSize(sv *settings.Values, ttl *catpb.RowLevelTTL) int64 {
	bs := ttl.DeleteBatchSize
	if bs == 0 {
		bs = defaultDeleteBatchSize.Get(sv)
	}
	return bs
}

// GetSelectRateLimit returns the table storage param value if specified or
// falls back to the cluster setting.
func GetSelectRateLimit(sv *settings.Values, ttl *catpb.RowLevelTTL) int64 {
	rl := ttl.SelectRateLimit
	if rl == 0 {
		rl = defaultSelectRateLimit.Get(sv)
	}
	// Put the maximum tokens possible if there is no rate limit.
	if rl == 0 {
		rl = math.MaxInt64
	}
	return rl
}

// GetDeleteRateLimit returns the table storage param value if specified or
// falls back to the cluster setting.
func GetDeleteRateLimit(sv *settings.Values, ttl *catpb.RowLevelTTL) int64 {
	rl := ttl.DeleteRateLimit
	if rl == 0 {
		rl = defaultDeleteRateLimit.Get(sv)
	}
	// Put the maximum tokens possible if there is no rate limit.
	if rl == 0 {
		rl = math.MaxInt64
	}
	return rl
}

// CheckJobEnabled returns nil if the job is enabled or an error if the job is
// disabled.
func CheckJobEnabled(settingsValues *settings.Values) error {
	if enabled := jobEnabled.Get(settingsValues); !enabled {
		return errors.Newf(
			"ttl jobs are currently disabled by CLUSTER SETTING %s",
			jobEnabled.Name(),
		)
	}
	return nil
}

// GetChangefeedReplicationDisabled returns whether changefeed replication
// should be disabled for this job based on the relevant cluster setting.
func GetChangefeedReplicationDisabled(
	settingsValues *settings.Values, ttl *catpb.RowLevelTTL,
) bool {
	if ttl.DisableChangefeedReplication {
		return true
	}
	return changefeedReplicationDisabled.Get(settingsValues)
}

// GetProcessorConcurrency returns the concurrency to use for TTL job processors.
// If the cluster setting is 0 (default), it will return the provided default value.
// If the cluster setting is greater than 0, it will return the minimum of the
// cluster setting and the default value.
func GetProcessorConcurrency(settingsValues *settings.Values, defaultConcurrency int64) int64 {
	override := processorConcurrencyOverride.Get(settingsValues)
	if override > 0 {
		return min(override, defaultConcurrency)
	}
	return defaultConcurrency
}

// BuildScheduleLabel returns a string value intended for use as the
// schedule_name/label column for the scheduled job created by row level TTL.
func BuildScheduleLabel(tbl *tabledesc.Mutable) string {
	return fmt.Sprintf("row-level-ttl: %s [%d]", tbl.GetName(), tbl.ID)
}

func BuildSelectQuery(
	relationName string,
	pkColNames []string,
	pkColDirs []catenumpb.IndexColumn_Direction,
	pkColTypes []*types.T,
	aostDuration time.Duration,
	ttlExpr catpb.Expression,
	numStartQueryBounds, numEndQueryBounds int,
	limit int64,
	startIncl bool,
) (string, error) {
	numPkCols := len(pkColNames)
	if numPkCols == 0 {
		panic("pkColNames is empty")
	}
	if numPkCols != len(pkColDirs) {
		panic("different number of pkColNames and pkColDirs")
	}
	var buf bytes.Buffer
	// SELECT
	buf.WriteString("SELECT ")
	for i := range pkColNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(pkColNames[i])
	}
	// FROM
	buf.WriteString("\nFROM ")
	buf.WriteString(relationName)
	// AS OF SYSTEM TIME
	buf.WriteString("\nAS OF SYSTEM TIME INTERVAL '")
	buf.WriteString(strconv.Itoa(int(aostDuration.Milliseconds()) / 1000))
	buf.WriteString(" seconds'")
	// WHERE
	buf.WriteString("\nWHERE ((")
	buf.WriteString(string(ttlExpr))
	buf.WriteString(") <= $1)")
	if numStartQueryBounds > 0 || numEndQueryBounds > 0 {
		buf.WriteString("\nAND ")
		const endPlaceholderOffset = 2
		clause, err := spanutils.RenderQueryBounds(pkColNames, pkColDirs, pkColTypes,
			numStartQueryBounds, numEndQueryBounds, startIncl, endPlaceholderOffset)
		if err != nil {
			return "", err
		}
		buf.WriteString(clause)
	}

	// ORDER BY
	buf.WriteString("\nORDER BY ")
	for i := range pkColNames {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(pkColNames[i])
		buf.WriteString(" ")
		buf.WriteString(pkColDirs[i].String())
	}
	// LIMIT
	buf.WriteString("\nLIMIT ")
	buf.WriteString(strconv.Itoa(int(limit)))
	return buf.String(), nil
}

func BuildDeleteQuery(
	relationName string, pkColNames []string, ttlExpr catpb.Expression, numRows int,
) string {
	if len(pkColNames) == 0 {
		panic("pkColNames is empty")
	}
	var buf bytes.Buffer
	// DELETE
	buf.WriteString("DELETE FROM ")
	buf.WriteString(relationName)
	// WHERE
	buf.WriteString("\nWHERE ((")
	buf.WriteString(string(ttlExpr))
	buf.WriteString(") <= $1)")
	if numRows > 0 {
		buf.WriteString("\nAND (")
		for i := range pkColNames {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(pkColNames[i])
		}
		buf.WriteString(") IN (")
		for i := 0; i < numRows; i++ {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString("(")
			for j := range pkColNames {
				if j > 0 {
					buf.WriteString(", ")
				}
				buf.WriteString("$")
				buf.WriteString(strconv.Itoa(i*len(pkColNames) + j + 2))
			}
			buf.WriteString(")")
		}
		buf.WriteString(")")
	}
	return buf.String()
}
