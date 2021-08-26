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
import { merge } from "lodash";
import { DOMAIN_NAME } from "../utils";
import { createSelector } from "reselect";
import { AppState } from "../reducers";

export type UIConfigState = {
  isTenant: boolean;
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
  isTenant: false,
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

export const selectUIConfig = createSelector(
  (state: AppState) => state.adminUI.uiConfig,
  uiConfig => uiConfig,
);

export const selectIsTenant = createSelector(
  selectUIConfig,
  uiConfig => uiConfig.isTenant,
);

export const { actions, reducer } = uiConfigSlice;
