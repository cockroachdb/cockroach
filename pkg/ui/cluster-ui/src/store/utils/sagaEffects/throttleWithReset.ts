// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { ActionPattern } from "@redux-saga/types";
import { ForkEffect } from "@redux-saga/core/effects";
import {
  actionChannel,
  cancel,
  delay,
  fork,
  race,
  take,
} from "redux-saga/effects";
import { buffers, Task } from "redux-saga";
import { Action } from "redux";

/***
 * Extended version of default `redux-saga/effects/throttle` effect implementation
 * with ability to provide actions that trigger reset on delay timer.
 *
 * For example, `initRequest` sata has to be throttled by specified amount of time and
 * also `initRequest` call when specific action dispatched (even if timeout doesn't
 * occur).
 *
 *    function* throttleApiCalls() {
 *      yield throttleWithReset(3000, "FETCH_DATA", "RESET", initRequest);
 *    }
 *
 *    function* initRequest(action) {
 *      yield put("INIT_REQUEST");
 *    }
 *
 *   In this example, someApi.get will be called at most once per 3 seconds, or when "FETCH_DATA"
 *   will be called after either "RESET", or "FAILED_REQUEST" actions.
 *
 *   delay (ms)     -----------X-----*-----------X-----------X-------->
 *   "FETCH_DATA"   --X--X--X-----X-----X----------------------------->
 *   "RESET"        -----------------X------------------------------->
 *   "INIT_REQUEST" --X-----------X-----X----------------------------->
 *                                   ^ delay is reset from this point
 *
 * @param ms time window to throttle and ignore incoming actions which match `pattern` param
 * @param pattern (redux-saga action pattern)
 * @param resetPattern matches actions which cause throttling timeout to reset.
 * @param task - generator function
 * */
export const throttleWithReset = <A extends Action>(
  ms: number,
  pattern: ActionPattern<A>,
  resetPattern: ActionPattern,
  task: (action: A) => any,
): ForkEffect<never> =>
  fork(function*() {
    // `actionChannel` creates a queue of the actions to process them sequentially.
    // Using `buffers.none()` allows to handle only single action and discard any actions
    // that arrive while current action is processed.
    const throttleChannel = yield actionChannel(pattern, buffers.none());
    const resetChannel = yield actionChannel(resetPattern, buffers.none());
    let t: Task;
    while (true) {
      const action = yield take(throttleChannel);
      // cancel previous task in order to handle only the most recent one.
      // it implements the behavior of `takeLatest` effect
      if (t) {
        yield cancel(t);
      }
      t = yield fork(task, action);
      yield race([delay(ms), take(resetChannel)]);
      // cancel forked task after timeout or cancellation action triggered
      yield cancel(t);
    }
  });
