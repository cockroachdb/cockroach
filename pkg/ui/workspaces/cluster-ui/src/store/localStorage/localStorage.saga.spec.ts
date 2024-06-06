// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { expectSaga, testSaga } from "redux-saga-test-plan";
import { takeEvery, takeLatest } from "redux-saga/effects";

import { actions as stmtInsightActions } from "src/store/insights/statementInsights/statementInsights.reducer";
import { actions as txnInsightActions } from "src/store/insights/transactionInsights/transactionInsights.reducer";
import { actions as sqlStatsActions } from "src/store/sqlStats/sqlStats.reducer";
import { actions as txnStatsActions } from "src/store/transactionStats";

import { defaultTimeScaleSelected } from "../../timeScaleDropdown";

import {
  localStorageSaga,
  updateLocalStorageItemSaga,
  updateTimeScale,
} from "./localStorage.saga";
import { actions } from "./localStorage.reducer";

const ts = defaultTimeScaleSelected;

describe("local storage sagas", () => {
  describe("localStorageSaga", () => {
    it("should fork relevant sagas on actions", () => {
      testSaga(localStorageSaga)
        .next()
        .all([
          takeEvery(actions.update, updateLocalStorageItemSaga),
          takeLatest(actions.updateTimeScale, updateTimeScale),
        ])
        .finish()
        .isDone();
    });
  });

  describe("updateTimeScale", () => {
    it("invalidates data depending on timescale ", () => {
      return expectSaga(updateTimeScale, actions.updateTimeScale({ value: ts }))
        .put(sqlStatsActions.invalidated())
        .put(stmtInsightActions.invalidated())
        .put(txnInsightActions.invalidated())
        .put(txnStatsActions.invalidated())
        .run();
    });
  });
});
