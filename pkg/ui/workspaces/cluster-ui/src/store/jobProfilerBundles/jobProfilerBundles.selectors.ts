// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { AppState } from "../reducers";
import { JobProfilerBundle } from "src/api/jobProfilerBundleApi";
import moment from "moment";
import { chain, orderBy } from "lodash";

export const jobProfilerBundles = createSelector(
    (state: AppState) => state.adminUI,
    state => state.jobProfilerBundles,
);

export const selectJobProfilerBundles = createSelector(
    jobProfilerBundles,
    state => state.data,
);

export type JobProfilerBundleDictionary = {
    [jobID: string]: JobProfilerBundle[];
};

export const selectJobProfilerBundlesPerJob = createSelector(
    selectJobProfilerBundles,
    (
        bundles: JobProfilerBundle[],
    ): JobProfilerBundleDictionary =>
        chain(bundles)
            .groupBy(bundle => bundle.job_id)
            // Perform DESC sorting to get latest report on top
            .mapValues(bundleValues =>
                orderBy(bundleValues, [b => moment(b.written).unix()], ["desc"]),
            )
            .value(),
);

export const selectJobProfilerBundlesByJobID = createSelector(
    selectJobProfilerBundles,
    (_state: AppState, jobID: string) => jobID,
    (requests, jobID) =>
        (requests || []).filter(
            request => request.job_id === jobID,
        ),
);