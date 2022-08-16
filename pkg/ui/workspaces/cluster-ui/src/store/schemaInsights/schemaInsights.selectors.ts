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
import { schemaInsightTypeToDisplayMapping } from "../../insights";
import { InsightRecommendation } from "../../insightsTable/insightsTable";

export const selectSchemaInsights = createSelector(
  adminUISelector,
  adminUiState => {
    if (!adminUiState.schemaInsights) return [];
    return adminUiState.schemaInsights.data;
  },
);

export const selectSchemaInsightsDatabases = createSelector(
  adminUISelector,
  adminUiState => {
    if (!adminUiState.schemaInsights || !adminUiState.schemaInsights.data)
      return [];
    const databases: Set<string> = new Set<string>();
    adminUiState.schemaInsights.data.map(schemaInsight => {
      databases.add(schemaInsight.database);
    });
    return Array.from(databases).sort();
  },
);

export const selectSchemaInsightsTypes = createSelector(
  adminUISelector,
  adminUiState => {
    if (!adminUiState.schemaInsights || !adminUiState.schemaInsights.data)
      return [];
    const schemaTypes: Set<string> = new Set<string>();
    adminUiState.schemaInsights.data.map(
      (schemaInsight: InsightRecommendation) => {
        schemaTypes.add(schemaInsightTypeToDisplayMapping[schemaInsight.type]);
      },
    );
    return Array.from(schemaTypes).sort();
  },
);
