import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { DOMAIN_NAME, noopReducer } from "../utils";

type StatementsResponse = cockroach.server.serverpb.StatementsResponse;

export type StatementsState = {
  data: StatementsResponse;
  lastError: Error;
  valid: boolean;
};

const initialState: StatementsState = {
  data: null,
  lastError: null,
  valid: true,
};

const statementsSlice = createSlice({
  name: `${DOMAIN_NAME}/statements`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<StatementsResponse>) => {
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

export const { reducer, actions } = statementsSlice;
