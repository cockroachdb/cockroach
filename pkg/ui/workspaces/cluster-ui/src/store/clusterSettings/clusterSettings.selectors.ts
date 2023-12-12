// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AppState } from "../reducers";
import { greaterOrEqualThanVersion, indexUnusedDuration } from "../../util";

export const selectAutomaticStatsCollectionEnabled = (
  state: AppState,
): boolean => {
  const settings = state.adminUI?.clusterSettings.data?.key_values;
  if (!settings) {
    return false;
  }
  const value = settings["sql.stats.automatic_collection.enabled"]?.value;
  return value === "true";
};

export const selectIndexRecommendationsEnabled = (state: AppState): boolean => {
  const settings = state.adminUI?.clusterSettings.data?.key_values;
  if (!settings) {
    return false;
  }
  const value = settings["version"]?.value || "";
  return greaterOrEqualThanVersion(value, [22, 2, 0]);
};

export const selectIndexUsageStatsEnabled = (state: AppState): boolean => {
  const settings = state.adminUI?.clusterSettings.data?.key_values;
  if (!settings) {
    return false;
  }
  const value = settings["version"]?.value || "";
  return greaterOrEqualThanVersion(value, [22, 1, 0]);
};

export const selectDropUnusedIndexDuration = (state: AppState): string => {
  const settings = state.adminUI?.clusterSettings.data?.key_values;
  if (!settings) {
    return indexUnusedDuration;
  }
  return (
    settings["sql.index_recommendation.drop_unused_duration"]?.value ||
    indexUnusedDuration
  );
};
