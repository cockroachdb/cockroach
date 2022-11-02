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
import { DOMAIN_NAME, noopReducer } from "../utils";
import {
  CancelStmtDiagnosticRequest,
  InsertStmtDiagnosticRequest,
  StatementDiagnosticsResponse,
} from "../../api";

export type StatementDiagnosticsState = {
  data: StatementDiagnosticsResponse;
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
      action: PayloadAction<StatementDiagnosticsResponse>,
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
    createReport: (
      _state,
      _action: PayloadAction<InsertStmtDiagnosticRequest>,
    ) => {},
    createReportCompleted: noopReducer,
    createReportFailed: (_state, _action: PayloadAction<Error>) => {},
    cancelReport: (
      _state,
      _action: PayloadAction<CancelStmtDiagnosticRequest>,
    ) => {},
    cancelReportCompleted: noopReducer,
    cancelReportFailed: (_state, _action: PayloadAction<Error>) => {},
    openNewDiagnosticsModal: (_state, _action: PayloadAction<string>) => {},
  },
});

export const { actions, reducer } = statementDiagnosticsSlice;
