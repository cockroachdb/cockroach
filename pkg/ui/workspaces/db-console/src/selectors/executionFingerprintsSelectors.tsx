// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AdminUIState } from "../redux/state";

// Statements
export const selectStatementsLastUpdated = (state: AdminUIState) =>
  state.cachedData.statements?.setAt?.utc();

export const selectStatementsDataValid = (state: AdminUIState) =>
  state.cachedData.statements?.valid;

export const selectStatementsDataInFlight = (state: AdminUIState) =>
  state.cachedData.statements?.inFlight;

// Transactions
export const selectTxnsLastUpdated = (state: AdminUIState) =>
  state.cachedData.transactions?.setAt?.utc();

export const selectTxnsDataValid = (state: AdminUIState) =>
  state.cachedData.transactions?.valid;

export const selectTxnsDataInFlight = (state: AdminUIState) =>
  state.cachedData.transactions?.inFlight;
