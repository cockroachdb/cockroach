import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
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
