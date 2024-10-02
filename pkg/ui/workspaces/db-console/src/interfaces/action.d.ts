// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
