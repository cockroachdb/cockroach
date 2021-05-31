// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/**
 * noopReducer is a stub function to use with `createSlice` (@redux-toolkit) as a definition
 * for reducer case which should not change state but it has to define an action which might be
 * handled in sagas for instance.
 *
 * @example
 * ```
 * const slice = createSlice({
 *  name: "someReducer",
 *  reducers: {
 *    someAction: noopReducer,
 *  },
 * });
 *
 * // then it is possible to access this action like this:
 * slice.actions.someAction()
 * ```
 * In this case, action with type "someReducer/someAction" is dispatched, can be handled
 * by middleware but it doesn't change state.
 */
export const noopReducer = (_state: unknown) => {};
