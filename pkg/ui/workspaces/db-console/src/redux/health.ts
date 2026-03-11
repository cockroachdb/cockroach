// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Action } from "redux";

import { AdminUIState } from "./state";

const SET_HEALTH_ERROR = "cockroachui/health/SET_ERROR";

export interface HealthState {
  lastError: Error | null;
}

interface SetHealthErrorAction extends Action {
  type: typeof SET_HEALTH_ERROR;
  error: Error | null;
}

export function setHealthError(error: Error | null): SetHealthErrorAction {
  return { type: SET_HEALTH_ERROR, error };
}

const initialState: HealthState = { lastError: null };

export function healthReducer(
  state: HealthState = initialState,
  action: SetHealthErrorAction,
): HealthState {
  switch (action.type) {
    case SET_HEALTH_ERROR:
      if (state.lastError === action.error) {
        return state;
      }
      return { lastError: action.error };
    default:
      return state;
  }
}

export const selectHealthLastError = (state: AdminUIState) =>
  state.health.lastError;
