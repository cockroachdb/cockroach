import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { DOMAIN_NAME, noopReducer } from "../utils";

type ILivenessResponse = cockroach.server.serverpb.ILivenessResponse;

export type LivenessState = {
  data: ILivenessResponse;
  lastError: Error;
  valid: boolean;
};

const initialState: LivenessState = {
  data: null,
  lastError: null,
  valid: true,
};

const nodesSlice = createSlice({
  name: `${DOMAIN_NAME}/liveness`,
  initialState,
  reducers: {
    received: (state, action: PayloadAction<ILivenessResponse>) => {
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

export const { reducer, actions } = nodesSlice;
