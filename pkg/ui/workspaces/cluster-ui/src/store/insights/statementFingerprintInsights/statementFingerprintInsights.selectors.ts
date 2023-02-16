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
import { AppState } from "src/store/reducers";

import { selectStatementFingerprintID } from "src/selectors/common";

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
