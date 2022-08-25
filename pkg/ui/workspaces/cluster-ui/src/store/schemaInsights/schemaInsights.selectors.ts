// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { adminUISelector } from "../utils/selectors";
import { insightType } from "../../insights";

export const selectSchemaInsights = createSelector(
  adminUISelector,
  adminUiState => {
    if (!adminUiState.schemaInsights) return [];
    return adminUiState.schemaInsights.data;
  },
);

export const selectSchemaInsightsDatabases = createSelector(
  selectSchemaInsights,
  schemaInsights => {
    if (!schemaInsights) return [];
    return Array.from(
      new Set(schemaInsights.map(schemaInsight => schemaInsight.database)),
    ).sort();
  },
);

export const selectSchemaInsightsTypes = createSelector(
  selectSchemaInsights,
  schemaInsights => {
    if (!schemaInsights) return [];
    return Array.from(
      new Set(
        schemaInsights.map(schemaInsight => insightType(schemaInsight.type)),
      ),
    ).sort();
  },
);
