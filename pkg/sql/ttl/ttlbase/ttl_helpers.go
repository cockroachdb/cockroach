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
)

var (
	startKeyCompareOps = map[catenumpb.IndexColumn_Direction]string{
		catenumpb.IndexColumn_ASC:  ">",
		catenumpb.IndexColumn_DESC: "<",
	}
	endKeyCompareOps = map[catenumpb.IndexColumn_Direction]string{
		catenumpb.IndexColumn_ASC:  "<",
		catenumpb.IndexColumn_DESC: ">",
	}
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

// BuildScheduleLabel returns a string value intended for use as the
// schedule_name/label column for the scheduled job created by row level TTL.
func BuildScheduleLabel(tbl *tabledesc.Mutable) string {
	return fmt.Sprintf("row-level-ttl: %s [%d]", tbl.GetName(), tbl.ID)
}

func BuildSelectQuery(
	relationName string,
	pkColNames []string,
	pkColDirs []catenumpb.IndexColumn_Direction,
	aostDuration time.Duration,
	ttlExpr catpb.Expression,
	numStartQueryBounds, numEndQueryBounds int,
	limit int64,
	startIncl bool,
) string {
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
	writeBounds := func(
		numQueryBounds int,
		placeholderOffset int,
		compareOps map[catenumpb.IndexColumn_Direction]string,
		inclusive bool,
	) {
		if numQueryBounds > 0 {
			buf.WriteString("\nAND (")
			for i := 0; i < numQueryBounds; i++ {
				isLast := i == numQueryBounds-1
				buf.WriteString("\n  (")
				for j := 0; j < i; j++ {
					buf.WriteString(pkColNames[j])
					buf.WriteString(" = $")
					buf.WriteString(strconv.Itoa(j + placeholderOffset))
					buf.WriteString(" AND ")
				}
				buf.WriteString(pkColNames[i])
				buf.WriteString(" ")
				buf.WriteString(compareOps[pkColDirs[i]])
				if isLast && inclusive {
					buf.WriteString("=")
				}
				buf.WriteString(" $")
				buf.WriteString(strconv.Itoa(i + placeholderOffset))
				buf.WriteString(")")
				if !isLast {
					buf.WriteString(" OR")
				}
			}
			buf.WriteString("\n)")
		}
	}
	const endPlaceholderOffset = 2
	writeBounds(
		numStartQueryBounds,
		endPlaceholderOffset+numEndQueryBounds,
		startKeyCompareOps,
		startIncl,
	)
	writeBounds(
		numEndQueryBounds,
		endPlaceholderOffset,
		endKeyCompareOps,
		true, /*inclusive*/
	)

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
	return buf.String()
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
