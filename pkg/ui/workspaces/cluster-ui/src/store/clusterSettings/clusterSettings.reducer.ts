// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";

import {
  SettingsRequestMessage,
  SettingsResponseMessage,
} from "../../api/clusterSettingsApi";
import { DOMAIN_NAME } from "../utils";

export type ClusterSettingsState = {
  data: SettingsResponseMessage;
  // Captures thrown errors.
  lastError: Error;
  valid: boolean;
  inFlight: boolean;
};

const initialState: ClusterSettingsState = {
  data: null,
  // Captures thrown errors.
  lastError: null,
  valid: false,
  inFlight: false,
};

const clusterSettingsReducer = createSlice({
  name: `${DOMAIN_NAME}/clustersettings`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<SettingsResponseMessage>) => {
      state.valid = true;
      state.inFlight = false;
      state.data = action.payload;
      state.lastError = null;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.inFlight = false;
      state.data = null;
      state.lastError = action.payload;
    },
    refresh: (_, _action: PayloadAction<SettingsRequestMessage>) => {},
    request: (_, _action: PayloadAction<SettingsRequestMessage>) => {},
  },
});

export const { reducer, actions } = clusterSettingsReducer;
