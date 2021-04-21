import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { merge } from "lodash";
import { DOMAIN_NAME } from "../utils";

export type UIConfigState = {
  pages: {
    statementDetails: {
      showStatementDiagnosticsLink: boolean;
    };
    sessionDetails: {
      showGatewayNodeLink: boolean;
    };
  };
};

const initialState: UIConfigState = {
  pages: {
    statementDetails: {
      showStatementDiagnosticsLink: true,
    },
    sessionDetails: {
      showGatewayNodeLink: false,
    },
  },
};

/**
 * `uiConfigSlice` is responsible to store configuration parameters which works as feature flags
 * and can be set dynamically by dispatching `update` action with updated configuration.
 * This might be useful in case client application that integrates some components or pages from
 * `cluster-ui` and has to exclude or add some extra logic on a page.
 **/
const uiConfigSlice = createSlice({
  name: `${DOMAIN_NAME}/uiConfig`,
  initialState,
  reducers: {
    update: (state, action: PayloadAction<Partial<UIConfigState>>) => {
      merge(state, action.payload);
    },
  },
});

export const { actions, reducer } = uiConfigSlice;
