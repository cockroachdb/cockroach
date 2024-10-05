// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package deprecatedshowranges exists to smoothen the transition
// between the pre-v23.1 semantics of SHOW RANGES and
// crdb_internal.ranges{_no_leases}, and the new semantics introduced
// in v23.1.
//
// The pre-v23.1 semantics are deprecated as of v23.1. At the end of
// the deprecation cycle (hopefully for v23.2) we expect to delete
// this package entirely and all the other code in the SQL layer that
// hangs off the EnableDeprecatedBehavior() conditional defined below.
//
// The mechanism to control the behavior is as follows:
//
//   - In any case, an operator can override the behavior using an env
//     var, "COCKROACH_FORCE_DEPRECATED_SHOW_RANGE_BEHAVIOR".
//     If set (to a boolean), the value of the env var is used in
//     all cases.
//     We use this env var to force the _new_ behavior through out
//     test suite, regardless of the other conditions. This allows
//     us to avoid maintaining two sets of outputs in tests.
//
//   - If the env var is not set, the cluster setting
//     `sql.show_ranges_deprecated_behavior.enabled` is used. It
//     defaults to true in v23.1 (the "smoothening" part).
//
//   - If the deprecated behavior is chosen by any of the above
//     mechanisms, and _the range coalescing cluster setting_ is set
//     to true, a loud warning will be reported in various places.
//     This will nudge users who wish to opt into range coalescing
//     to adapt their use of the range inspection accordingly.
package deprecatedshowranges

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigstore"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// EnableDeprecatedBehavior chooses whether to use the deprecated
// interface and output for crdb_internal.ranges{_no_leases} and SHOW
// RANGES. See the package-level doc above for details.
func EnableDeprecatedBehavior(
	ctx context.Context, st *cluster.Settings, ns eval.ClientNoticeSender,
) bool {
	useDeprecatedBehavior := false
	if fb.behaviorIsForcedByEnvVar {
		useDeprecatedBehavior = fb.useDeprecatedBehavior
	} else {
		useDeprecatedBehavior = ShowRangesDeprecatedBehaviorSetting.Get(&st.SV)
	}

	if useDeprecatedBehavior {
		var err pgnotice.Notice
		if spanconfigstore.StorageCoalesceAdjacentSetting.Get(&st.SV) {
			err = pgnotice.Newf("deprecated use of SHOW RANGES or crdb_internal.ranges{,_no_leases} " +
				"in combination with range coalescing - expect invalid results")
			// Tell the operator about it in logs.
			log.Warningf(ctx, "%v", err)
		} else {
			err = pgnotice.Newf("attention! the pre-23.1 behavior of SHOW RANGES and crdb_internal.ranges{,_no_leases} is deprecated!")
		}
		err = errors.WithHint(
			err,
			"Consider enabling the new functionality by setting "+
				"'sql.show_ranges_deprecated_behavior.enabled' to 'false'.\n"+
				"The new SHOW RANGES statement has more options. "+
				"Refer to the online documentation or execute 'SHOW RANGES ??' for details.")
		if ns != nil {
			// Tell the SQL client about it via a NOTICE message.
			ns.BufferClientNotice(ctx, err)
		}
	}

	return useDeprecatedBehavior
}

// ShowRangesDeprecatedBehaviorSettingName is the name of the cluster
// setting that controls the behavior.
const ShowRangesDeprecatedBehaviorSettingName = "sql.show_ranges_deprecated_behavior.enabled"

// ShowRangesDeprecatedBehaviorSetting is the setting that controls
// the behavior. Exported for use in tests.
var ShowRangesDeprecatedBehaviorSetting = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	ShowRangesDeprecatedBehaviorSettingName,
	"if set, SHOW RANGES and crdb_internal.ranges{_no_leases} behave with deprecated pre-v23.1 semantics."+
		" NB: the new SHOW RANGES interface has richer WITH options "+
		"than pre-v23.1 SHOW RANGES.",
	false,
	settings.WithPublic)

const envVarName = "COCKROACH_FORCE_DEPRECATED_SHOW_RANGE_BEHAVIOR"

type forcedBehavior struct {
	behaviorIsForcedByEnvVar bool
	useDeprecatedBehavior    bool
}

var fb = func() forcedBehavior {
	selection, hasEnvVar := envutil.EnvString(envVarName, 1)
	if !hasEnvVar {
		return forcedBehavior{behaviorIsForcedByEnvVar: false}
	}
	v, err := strconv.ParseBool(selection)
	if err != nil {
		panic(fmt.Sprintf("error parsing %s: %s", envVarName, err))
	}
	return forcedBehavior{behaviorIsForcedByEnvVar: true, useDeprecatedBehavior: v}
}()
