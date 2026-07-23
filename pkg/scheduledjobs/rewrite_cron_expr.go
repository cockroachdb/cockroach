// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scheduledjobs

import (
	"fmt"
	"hash/fnv"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

const (
	cronWeekly = "@weekly"
	cronDaily  = "@daily"
	cronHourly = "@hourly"
)

// MaybeRewriteCronExpr is used to rewrite the interval-oriented cron exprs
// into an equivalent frequency interval but with an offset derived from the
// uuid. For a given pair of inputs, the output of this function will always
// be the same. If the input cronExpr is not a special form as denoted by
// the keys of cronExprRewrites, it will be returned unmodified. This rewrite
// occurs in order to uniformly distribute the production of telemetry logs
// over the intended time interval to avoid bursts.
func MaybeRewriteCronExpr(id uuid.UUID, cronExpr string) string {
	if f, ok := cronExprRewrites[cronExpr]; ok {
		hash := fnv.New64a() // arbitrary hash function
		_, _ = hash.Write(id.GetBytes())
		return f(rand.New(rand.NewSource(int64(hash.Sum64()))))
	}
	return cronExpr
}

var cronExprRewrites = map[string]func(r *rand.Rand) string{
	cronWeekly: func(r *rand.Rand) string {
		return fmt.Sprintf("%d %d * * %d", r.Intn(60), r.Intn(23), r.Intn(7))
	},
	cronDaily: func(r *rand.Rand) string {
		return fmt.Sprintf("%d %d * * *", r.Intn(60), r.Intn(23))
	},
	cronHourly: func(r *rand.Rand) string {
		return fmt.Sprintf("%d * * * *", r.Intn(60))
	},
}
