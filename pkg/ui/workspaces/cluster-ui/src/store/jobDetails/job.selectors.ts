// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { adminUISelector } from "src/store/utils/selectors";
import { selectID } from "src/selectors";
import { JobResponse } from "src/api/jobsApi";

const selectJobState = createSelector(adminUISelector, state => {
  const jobState = state?.job?.cachedData;
  const emptyJobCache = !jobState || Object.keys(jobState).length === 0;
  if (emptyJobCache) {
    return null;
  }
  return jobState;
});

export const selectJob = createSelector(
  [adminUISelector, selectJobState, selectID],
  (adminUIState, jobState, jobID) => {
    const jobsCache = adminUIState?.jobs?.data;
    let job: JobResponse;
    if (!jobID || (!jobsCache && !jobState)) {
      return null;
    } else if (jobsCache) {
      job = Object(jobsCache.jobs.find(job => job.id.toString() === jobID));
    } else if (jobState) {
      job = jobState[jobID]?.data;
    }
    return job;
  },
);

export const selectJobError = createSelector(
  selectJobState,
  selectID,
  (state, jobID) => {
    if (!state || !jobID) {
      return null;
    }
    return state[jobID]?.lastError;
  },
);

export const selectJobLoading = createSelector(
  selectJobState,
  selectID,
  (state, jobID) => {
    if (!state || !jobID) {
      return null;
    }
    return state[jobID]?.inFlight;
  },
);
