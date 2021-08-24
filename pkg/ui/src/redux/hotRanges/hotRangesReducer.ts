// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { GetHotRangesResponseMessage } from "oss/src/util/api";

export interface HotRangesState {
  loading: boolean;
  data?: GetHotRangesResponseMessage;
  error?: Error;
}

const INITIAL_STATE = {
  loading: true,
};

export default function (state = INITIAL_STATE, action: any): HotRangesState {
  switch (action.type) {
    case "GET_HOT_RANGES_SUCCEEDED":
      return {
        loading: false,
        data: action.payload,
      };
    case "GET_HOT_RANGES_FAILED":
      return {
        ...state,
        loading: false,
        error: action.payload,
      };
    default:
      return state;
  }
}
