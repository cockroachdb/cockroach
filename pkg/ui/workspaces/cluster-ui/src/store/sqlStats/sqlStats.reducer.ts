// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";

import { TimeScale } from "../../timeScaleDropdown";
import { DOMAIN_NAME } from "../utils";

export type UpdateTimeScalePayload = {
  ts: TimeScale;
};

export type SQLStatsState = Record<string, never>;

const initialState: SQLStatsState = {};

const sqlStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/sqlstats`,
  initialState,
  reducers: {
    updateTimeScale: (_, _action: PayloadAction<UpdateTimeScalePayload>) => {},
    invalidated: () => {},
  },
});

export const { reducer, actions } = sqlStatsSlice;
