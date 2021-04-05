import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { DOMAIN_NAME, noopReducer } from "../utils";

type ResetSQLStatsResponse = cockroach.server.serverpb.ResetSQLStatsResponse;

export type ResetSQLStatsState = {
  data: ResetSQLStatsResponse;
  lastError: Error;
  valid: boolean;
};

const initialState: ResetSQLStatsState = {
  data: null,
  lastError: null,
  valid: true,
};

const resetSQLStatsSlice = createSlice({
  name: `${DOMAIN_NAME}/resetsqlstats`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<ResetSQLStatsResponse>) => {
      state.data = action.payload;
      state.valid = true;
      state.lastError = null;
    },
    failed: (state, action: PayloadAction<Error>) => {
      state.valid = false;
      state.lastError = action.payload;
    },
    invalidated: state => {
      state.valid = false;
    },
    // Define actions that don't change state
    refresh: noopReducer,
    request: noopReducer,
  },
});

export const { reducer, actions } = resetSQLStatsSlice;
