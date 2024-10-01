// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { createSlice, PayloadAction } from "@reduxjs/toolkit";

import { DOMAIN_NAME, noopReducer } from "../utils";

import { ICancelQueryRequest, ICancelSessionRequest } from ".";

type CancelQueryResponse = cockroach.server.serverpb.CancelQueryResponse;

export type TerminateQueryState = {
  data: CancelQueryResponse;
  lastError: Error;
  valid: boolean;
};

const initialState: TerminateQueryState = {
  data: null,
  lastError: null,
  valid: true,
};

const terminateQuery = createSlice({
  name: `${DOMAIN_NAME}/terminateQuery`,
  initialState,
  reducers: {
    terminateSession: (
      _state,
      _action: PayloadAction<ICancelSessionRequest>,
    ) => {},
    terminateSessionCompleted: noopReducer,
    terminateSessionFailed: (_state, _action: PayloadAction<Error>) => {},
    terminateQuery: (_state, _action: PayloadAction<ICancelQueryRequest>) => {},
    terminateQueryCompleted: noopReducer,
    terminateQueryFailed: (_state, _action: PayloadAction<Error>) => {},
  },
});

export const { reducer, actions } = terminateQuery;
