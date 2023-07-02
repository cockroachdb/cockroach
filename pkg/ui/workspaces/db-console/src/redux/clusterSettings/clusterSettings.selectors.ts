// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import { AdminUIState } from "src/redux/state";
import { cockroach } from "src/js/protos";
import moment from "moment-timezone";
import { CoordinatedUniversalTime, util } from "@cockroachlabs/cluster-ui";

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

export const selectCrossClusterReplicationEnabled = createSelector(
  selectClusterSettings,
  (settings): boolean | undefined => {
    if (!settings) {
      return undefined;
    }
    const value = settings["cross_cluster_replication.enabled"]?.value;
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
