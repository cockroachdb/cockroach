// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { CoordinatedUniversalTime, util } from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";
import { createSelector } from "reselect";

import { cockroach } from "src/js/protos";
import { AdminUIState } from "src/redux/state";
import { indexUnusedDuration } from "src/util/constants";

export const selectClusterSettings = createSelector(
  (state: AdminUIState) => state.cachedData.settings?.data,
  (settings: cockroach.server.serverpb.SettingsResponse) =>
    settings?.key_values,
);

export const selectTimezoneSetting = createSelector(
  selectClusterSettings,
  settings => {
    if (!settings) {
      return CoordinatedUniversalTime;
    }
    return settings["ui.display_timezone"]?.value || CoordinatedUniversalTime;
  },
);

export const selectResolution10sStorageTTL = createSelector(
  selectClusterSettings,
  (settings): moment.Duration | undefined => {
    if (!settings) {
      return undefined;
    }
    const value = settings["timeseries.storage.resolution_10s.ttl"]?.value;
    return util.durationFromISO8601String(value);
  },
);

export const selectResolution30mStorageTTL = createSelector(
  selectClusterSettings,
  settings => {
    if (!settings) {
      return undefined;
    }
    const value = settings["timeseries.storage.resolution_30m.ttl"]?.value;
    return util.durationFromISO8601String(value);
  },
);

export const selectAutomaticStatsCollectionEnabled = createSelector(
  selectClusterSettings,
  (settings): boolean | undefined => {
    if (!settings) {
      return undefined;
    }
    const value = settings["sql.stats.automatic_collection.enabled"]?.value;
    return value === "true";
  },
);

export const selectIndexRecommendationsEnabled = createSelector(
  selectClusterSettings,
  (settings): boolean => {
    if (!settings) {
      return false;
    }
    const value = settings["version"]?.value || "";
    return util.greaterOrEqualThanVersion(value, [22, 2, 0]);
  },
);

export const selectClusterSettingVersion = createSelector(
  selectClusterSettings,
  (settings): string => {
    if (!settings) {
      return "";
    }
    return settings["version"].value;
  },
);

export const selectIndexUsageStatsEnabled = createSelector(
  selectClusterSettings,
  (settings): boolean => {
    if (!settings) {
      return false;
    }
    const value = settings["version"]?.value || "";
    return util.greaterOrEqualThanVersion(value, [22, 1, 0]);
  },
);

export const selectDropUnusedIndexDuration = createSelector(
  selectClusterSettings,
  (settings): string => {
    if (!settings) {
      return indexUnusedDuration;
    }
    return (
      settings["sql.index_recommendation.drop_unused_duration"]?.value ||
      indexUnusedDuration
    );
  },
);
