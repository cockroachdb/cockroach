// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { all, call, put, delay, takeLatest } from "redux-saga/effects";
import { actions } from "./keyVizualizerReducer";
import * as api from "src/util/api";
import { SagaIterator } from "@redux-saga/types";
import { PayloadAction } from "src/interfaces/action";
import { useSelector } from "react-redux";
import { selectTimeScaleCurrentWindow } from "./keyVizualizerSelectors";

export type KeyVizualizerCellInfoRequest = {
  startKey: number;
  endKey: number;
  sampleTime: number;
};

export function* requestKeyVizualizerCellInfo(
  action: PayloadAction<KeyVizualizerCellInfoRequest>,
): SagaIterator {
  try {
    //const { start, end } = useSelector(selectTimeScaleCurrentWindow);
    //const request = new KeyVizualizerCellInfoRequest(...action.payload, start, end);
    //const result = yield call(api.getKeyVizualizerCellInfo, request);
    const result = mockResponse;
    yield put(
      actions.receivedCellInfo({
        ...result,
        sampleTime: action.payload.sampleTime,
      }),
    );
  } catch (e) {
    yield put(actions.failedCellInfo(e));
  }
}

export function* keyVizualizerSaga() {
  yield all([
    takeLatest(actions.requestCellInfo, requestKeyVizualizerCellInfo),
  ]);
}

export const mockResponse = {
  schema: {
    database: "system",
    table: "lease",
  },
  rangeId: 1,
  qps: 2230.483939,
  startKey: "Table/100/1/1",
  endKey: "Table/200/5/5",
  nodes: [32],
  store: 32,
  locality: "locality",
  keyBytes: 5184,
  leaseholder: 117,
  index: "idx1_lease",
};
