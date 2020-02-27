// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Action } from "redux";

import { PayloadAction } from "src/interfaces/action";

export const ENQUEUE_DIAGNOSTICS = "cockroachui/statements/ENQUEUE_DIAGNOSTICS";
export const ENQUEUE_DIAGNOSTICS_COMPLETE = "cockroachui/statements/ENQUEUE_DIAGNOSTICS_COMPLETE";

export type EnqueueDiagnosticsPayload = {
  statementId: string;
};

export function enqueueDiagnostics(statementId: string): PayloadAction<EnqueueDiagnosticsPayload> {
  return {
    type: ENQUEUE_DIAGNOSTICS,
    payload: {
      statementId,
    },
  };
}

export function completeEnqueueDiagnostics(): Action {
  return {
    type: ENQUEUE_DIAGNOSTICS_COMPLETE,
  };
}
