// Copyright 2022 The Cockroach Authors.
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
import { DOMAIN_NAME } from "../utils";
import { StatementDetailsRequest } from "src/api/statementsApi";
import { generateStmtDetailsToID } from "../../statementDetails/statementDetails.selectors";

type StatementDetailsResponse = cockroach.server.serverpb.StatementDetailsResponse;

export type SQLDetailsStatsState = {
  data: StatementDetailsResponse;
  lastError: Error;
  valid: boolean;
};

export type SQLDetailsStatsReducerState = {
  [id: string]: SQLDetailsStatsState;
};

const initialState: SQLDetailsStatsReducerState = {};
// TODO marylia fix here
const sqlDetailsStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/sqlDetailsStats`,
  initialState,
  reducers: {
    received(state, action: PayloadAction<StatementDetailsResponse>) {
      const stmt = action.payload.statement;
      const key = generateStmtDetailsToID(
        stmt.key_data.query,
        stmt.app_names.join(","),
      );
      console.log("PAYLOAD RECEIVED ", action.payload);
      state[key].data = action.payload;
      state[key].valid = true;
      state[key].lastError = null;
    },
    failed: (state, action: PayloadAction<Error>) => {
      console.log("PAYLOAD ERROR ", action.payload);
      const key = "";
      state[key].valid = false;
      state[key].lastError = action.payload;
    },
    invalidated: state => {
      console.log("invalidate ", state);
      const key = "";
      state[key].valid = false;
    },
    refresh: (_, action?: PayloadAction<StatementDetailsRequest>) => {},
    request: (_, action?: PayloadAction<StatementDetailsRequest>) => {},
  },
});

export const { reducer, actions } = sqlDetailsStatsSlice;
