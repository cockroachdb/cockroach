// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AdminUIState } from "../redux/state";

// Transactions
export const selectTxnsLastUpdated = (state: AdminUIState) =>
  state.cachedData.transactions?.setAt?.utc();

export const selectTxnsDataValid = (state: AdminUIState) =>
  state.cachedData.transactions?.valid;

export const selectTxnsDataInFlight = (state: AdminUIState) =>
  state.cachedData.transactions?.inFlight;
