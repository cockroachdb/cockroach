// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { selectStatementFingerprintID } from "src/selectors/common";
import { AppState } from "src/store/reducers";

export const selectStatementFingerprintInsights = createSelector(
  (state: AppState) => state.adminUI?.statementFingerprintInsights?.cachedData,
  selectStatementFingerprintID,
  (cachedFingerprintInsights, fingerprintID) => {
    if (!cachedFingerprintInsights) {
      return null;
    }
    return cachedFingerprintInsights[fingerprintID]?.data?.results;
  },
);
