// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { insightType } from "../../insights";
import { adminUISelector, localStorageSelector } from "../utils/selectors";

const selectSchemaInsightState = createSelector(
  adminUISelector,
  adminUiState => {
    if (!adminUiState.schemaInsights) return null;
    return adminUiState?.schemaInsights;
  },
);

export const selectSchemaInsights = createSelector(
  selectSchemaInsightState,
  schemaInsightState => {
    if (!schemaInsightState.data) return null;
    return schemaInsightState.data?.results;
  },
);

export const selectSchemaInsightsError = createSelector(
  selectSchemaInsightState,
  schemaInsightState => {
    if (!schemaInsightState) return null;
    return schemaInsightState.lastError;
  },
);

export const selectSchemaInsightsMaxApiSizeReached = createSelector(
  selectSchemaInsightState,
  schemaInsightState => {
    if (!schemaInsightState.data) return false;
    return schemaInsightState.data?.maxSizeReached;
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

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/SchemaInsightsPage"],
);

export const selectFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/SchemaInsightsPage"],
);
