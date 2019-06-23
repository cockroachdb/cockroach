// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Action } from "redux";

/**
 * PayloadAction implements the very common case of an action that includes a
 * single data object as a payload.
 */
export interface PayloadAction<T> extends Action {
  payload: T;
}

/**
 * WithRequest implements the very common case of an action payload that has an
 * associated Request.
 */
interface WithRequest<T, R> {
  data?: T;
  request: R;
}
