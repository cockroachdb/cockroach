// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AdminUIState } from "src/redux/state";
import { createSelector } from "reselect";
import { cockroach } from "src/js/protos";

const hotRangesState = (state: AdminUIState) => state.cachedData.hotRanges;

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
