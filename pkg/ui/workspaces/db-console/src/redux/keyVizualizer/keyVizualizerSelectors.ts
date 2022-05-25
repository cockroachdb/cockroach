// Copyright 2022 The Cockroach Authors.
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

export const selectCellInfoSlice = (state: AdminUIState) =>
  state.keyVizualizer?.cellInfoData;

export const selectTimeScaleCurrentWindow = (state: AdminUIState) =>
  state.timeScale.metricsTime?.currentWindow;

export const selectCellInfoByKey = (
  startKey: string,
  endKey: string,
  sampleTime: number,
) =>
  createSelector(selectCellInfoSlice, cellInfoState =>
    cellInfoState?.data?.find(
      cellInfo =>
        cellInfo.startKey === startKey &&
        cellInfo.endKey === endKey &&
        cellInfo.sampleTime === sampleTime,
    ),
  );
