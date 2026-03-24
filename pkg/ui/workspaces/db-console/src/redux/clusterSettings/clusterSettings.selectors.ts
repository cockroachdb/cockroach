// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { cockroach } from "src/js/protos";
import { AdminUIState } from "src/redux/state";

export const selectClusterSettings = createSelector(
  (state: AdminUIState) => state.cachedData.settings?.data,
  (settings: cockroach.server.serverpb.SettingsResponse) =>
    settings?.key_values,
);

export const selectClusterSettingVersion = createSelector(
  selectClusterSettings,
  (settings): string => {
    if (!settings) {
      return "";
    }
    return settings["version"]?.value ?? "";
  },
);
