// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/**
 * This module maintains the state of CockroachDB time series queries needed by
 * the web application. Cached query data is maintained separately for
 * individual components (e.g. different graphs); components are distinguished
 * in the reducer by a unique ID.
 */

import { createSlice, PayloadAction } from "@reduxjs/toolkit";

export type KeyVizualizerState = {
  mainData: {
    data: any[];
    error: Error;
    loading: boolean;
  };
  cellInfoData: {
    data: CellInfoState[];
    error: Error;
    loading: boolean;
  };
};

// Remove when CellInfoResponse is defined.
export type CellInfoState = {
  schema: {
    database: string;
    table: string;
  };
  rangeId: number;
  qps: number;
  startKey: string;
  endKey: string;
  sampleTime: number;
  nodes: number[];
  store: number;
  locality: string;
  keyBytes: number;
  leaseholder: number;
  index: string;
};

const initialState: KeyVizualizerState = {
  mainData: {
    data: [],
    error: null,
    loading: false,
  },
  cellInfoData: {
    data: [],
    error: null,
    loading: false,
  },
};

const keyVizualizerSlice = createSlice({
  name: "keyVizualizer",
  initialState,
  reducers: {
    requestCellInfo: (state, _) => {
      state.cellInfoData.loading = true;
      state.cellInfoData.error = null;
    },
    receivedCellInfo: (state, action) => {
      state.cellInfoData.data = state.cellInfoData.data.concat(action.payload);
      state.cellInfoData.loading = false;
      state.cellInfoData.error = null;
    },
    failedCellInfo: (state, action: PayloadAction<Error>) => {
      state.cellInfoData.loading = false;
      state.cellInfoData.error = action.payload;
    },
    clearCellInfo: state => {
      state.cellInfoData.data = [];
    },
  },
});

export const { reducer, actions } = keyVizualizerSlice;
