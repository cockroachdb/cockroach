// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";

import { cockroach } from "src/js/protos";
import { AdminUIState } from "src/redux/state";

import { LocalSetting } from "./localsettings";

const hotRangesState = (state: AdminUIState) => state.cachedData.hotRanges;
const localSettingsSelector = (state: AdminUIState) => state.localSettings;

export const hotRangesSelector = createSelector(hotRangesState, hotRanges =>
  Object.values(hotRanges?.data || {})
    .reduce<cockroach.server.serverpb.HotRangesResponseV2["ranges"]>(
      (acc, v) => [...acc, ...v.ranges],
      [],
    )
    // filter out ranges with 0 QPS
    .filter(v => v?.qps && v.qps > 0),
);

export const lastErrorSelector = createSelector(
  hotRangesState,
  hotRanges => hotRanges?.lastError,
);

export const isValidSelector = createSelector(
  hotRangesState,
  hotRanges => hotRanges?.valid,
);

export const lastSetAtSelector = createSelector(
  hotRangesState,
  hotRanges => hotRanges?.setAt,
);

export const isLoadingSelector = createSelector(
  hotRangesState,
  hotRanges => hotRanges?.inFlight,
);

export const sortSettingLocalSetting = new LocalSetting(
  "sortSetting/hotRanges",
  localSettingsSelector,
  { ascending: false, columnTitle: "qps" },
);
