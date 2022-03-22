// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";

/**
 * nextState is a utility function that allows type-safe replacement of fields
 * when generating a new state in a redux reducer. This is an alternative to
 * using the spread operator; e.g. instead of:
 *
 * return {
 *   ...state,
 *   prop1: "newValue",
 * }
 *
 * nextState can be used instead:
 *
 * return nextState(state, {
 *   prop1: "newValue",
 * });
 *
 * The advantage is the explicit requirement that replacement values are
 * overwriting fields that exist on the type of state. In the examples above,
 * using the spread operator would compile even if "prop1" was not a field of
 * state's type. This is an explicit design choice of typescript.
 *
 * @param lastState An object representing the previous state of a reducer.
 * @param changes A set of new properties which should replace properties of the
 * previous object.
 */
export default function nextState<T extends Object>(
  lastState: T,
  changes: Partial<T>,
): T {
  return _.assign({}, lastState, changes);
}
