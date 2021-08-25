// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Action } from "redux";

export const GET_HOT_RANGES = "GET_HOT_RANGES";
export const GET_HOT_RANGES_SUCCEEDED = "GET_HOT_RANGES_SUCCEEDED";
export const GET_HOT_RANGES_FAILED = "GET_HOT_RANGES_FAILED";

export interface GetHotRangesAction extends Action {
  payload: string;
}

export interface GetHotRangesSucceededAction extends Action {
  payload: string;
}

export interface GetHotRangesFailedAction extends Action {
  payload: Error;
}

export function getHotRangesAction(payload?: string) {
  return {
    type: GET_HOT_RANGES,
    payload,
  };
}

export function getHotRangesSucceededAction(payload: any) {
  return {
    type: GET_HOT_RANGES_SUCCEEDED,
    payload,
  };
}

export function getHotRangesFailedAction(payload: Error) {
  return {
    type: GET_HOT_RANGES_FAILED,
    payload,
  };
}
