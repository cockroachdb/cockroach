// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { DOMAIN_NAME, noopReducer } from "../utils";

type StatementDiagnosticsReportsResponse = cockroach.server.serverpb.StatementDiagnosticsReportsResponse;

export type StatementDiagnosticsState = {
  data: StatementDiagnosticsReportsResponse;
  lastError: Error;
  valid: boolean;
};

const initialState: StatementDiagnosticsState = {
  data: null,
  valid: true,
  lastError: null,
};

const statementDiagnosticsSlice = createSlice({
  name: `${DOMAIN_NAME}/statementDiagnostics`,
  initialState,
  reducers: {
    received: (
      state: StatementDiagnosticsState,
      action: PayloadAction<StatementDiagnosticsReportsResponse>,
    ) => {
      state.data = action.payload;
      state.lastError = null;
      state.valid = true;
    },
    failed: (
      state: StatementDiagnosticsState,
      action: PayloadAction<Error>,
    ) => {
      state.lastError = action.payload;
      state.valid = false;
    },
    refresh: noopReducer,
    request: noopReducer,
    invalidated: noopReducer,
    createReport: (_state, _action: PayloadAction<string>) => {},
    createReportCompleted: noopReducer,
    createReportFailed: (_state, _action: PayloadAction<Error>) => {},
    openNewDiagnosticsModal: (_state, _action: PayloadAction<string>) => {},
  },
});

export const { actions, reducer } = statementDiagnosticsSlice;
