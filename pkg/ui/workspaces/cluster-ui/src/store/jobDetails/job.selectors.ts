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

export const selectJobState = createSelector(
  adminUISelector,
  selectID,
  (state, jobID) => {
    const jobCache = state?.job?.cachedData;
    if (!jobCache || !jobID) {
      return null;
    }
    return jobCache[jobID];
  },
);
