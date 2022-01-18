// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "src/js/protos";
import { getHotRanges } from "src/util/api";
import { all, call, put, takeEvery } from "redux-saga/effects";
import {
  getHotRangesFailedAction,
  getHotRangesSucceededAction,
  GET_HOT_RANGES,
} from "./hotRangesActions";

import HotRangesRequest = cockroach.server.serverpb.HotRangesRequest;
import HotRangesResponse = cockroach.server.serverpb.HotRangesResponseV2;
import IHotRangesRequest = cockroach.server.serverpb.IHotRangesRequest;

import { PayloadAction } from "oss/src/interfaces/action";

export function* getHotRangesSaga(action: PayloadAction<IHotRangesRequest>) {
  const hotRangesRequest = new HotRangesRequest(action.payload);
  try {
    const resp: HotRangesResponse = yield call(getHotRanges, hotRangesRequest);
    yield put(getHotRangesSucceededAction(resp?.ranges));
  } catch (e) {
    yield put(getHotRangesFailedAction(e));
  }
}

export function* hotRangesSaga() {
  yield all([takeEvery(GET_HOT_RANGES, getHotRangesSaga)]);
}
