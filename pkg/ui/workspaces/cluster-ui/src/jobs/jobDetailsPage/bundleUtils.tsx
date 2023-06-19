
// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import moment from "moment";
import { JobProfilerBundle } from "src/api/jobProfilerBundleApi";
import { TimeScale, toDateRange } from "src/timeScaleDropdown";

export function filterByTimeScale(
    bundles: JobProfilerBundle[],
    ts: TimeScale,
): JobProfilerBundle[] {
    const [start, end] = toDateRange(ts);
    return bundles.filter(
        bundle =>
            start.isSameOrBefore(moment(bundle.written)) &&
            end.isSameOrAfter(moment(bundle.written)),
    );
}