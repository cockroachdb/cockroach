// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/**
 * The local settings reducer is designed to store local-only UI settings in
 * redux state. These settings are maintained within a session, but not saved
 * between sessions.
 *
 * This is appropriate for use by components which have some local state that is
 * not relevant to any other components in the application; for example, the
 * sort setting of a table. If a value is shared by multiple components,
 * it should be given the full redux treatment with unique modification actions.
 */

import { util } from "@cockroachlabs/cluster-ui";
import clone from "lodash/clone";
import isNil from "lodash/isNil";
import { Action } from "redux";
import { call, takeEvery } from "redux-saga/effects";
import { createSelector, Selector } from "reselect";

import { PayloadAction } from "src/interfaces/action";

const STORAGE_PREFIX = "cockroachui";
export const SET_UI_VALUE = `${STORAGE_PREFIX}/ui/SET_UI_VALUE`;

export interface LocalSettingData {
  key: string;
  value: any;
}

/**
 * Local settings are stored in a simple string-keyed dictionary.
 */
export interface LocalSettingsState {
  [key: string]: any;
}

/**
 * Persist local setting value in sessionStorage.
 * Append STORAGE_PREFIX to organize keys in a group.
 */
function saveToSessionStorage(data: LocalSettingData) {
  const value = JSON.stringify(data.value);
  // Silently handle possible exception when saving data to sessionStorage.
  // It is possible that sessionStorage is full, so it is not
  // possible to persist data in it.
  try {
    sessionStorage.setItem(`${STORAGE_PREFIX}/${data.key}`, value);
  } catch (e) {
    // eslint-disable-next-line no-console
    console.warn(util.maybeError(e).message);
  }
}

/**
 * Retrieve local setting value by key from sessionStorage.
 * Value is stored as a stringified JSON so has to be parsed back.
 */
export function getValueFromSessionStorage(key: string) {
  const value = sessionStorage.getItem(`${STORAGE_PREFIX}/${key}`);
  return JSON.parse(value);
}

/**
 * reducer function which handles local settings, storing them in a dictionary.
 */
export function localSettingsReducer(
  state: LocalSettingsState = {},
  action: Action,
): LocalSettingsState {
  if (isNil(action)) {
    return state;
  }

  switch (action.type) {
    case SET_UI_VALUE: {
      const { payload } = action as PayloadAction<LocalSettingData>;
      state = clone(state);
      state[payload.key] = payload.value;
      return state;
    }
    default:
      return state;
  }
}

/**
 * Action creator to set a named local setting.
 */
export function setLocalSetting(
  key: string,
  value: any,
): PayloadAction<LocalSettingData> {
  return {
    type: SET_UI_VALUE,
    payload: {
      key: key,
      value: value,
    },
  };
}

/**
 * LocalSetting is a wrapper class which provides type safety when accessing UI
 * settings. Components that use a local setting should instantiate this class
 * to access or modify it.
 */
export class LocalSetting<S, T> {
  private _value: Selector<S, T>;

  /**
   * Action creator which will create or overwrite this setting when dispatched
   * @param value The new value of the setting
   */
  set = (value: T) => {
    return setLocalSetting(this.key, value);
  };

  /**
   * Selector which retrieves this setting from the LocalSettingsState
   * @param state The current top-level redux state of the application.
   */
  selector = (state: S) => {
    return this._value(state);
  };

  /**
   * Selector which retrieves this setting from the LocalSettingsState
   * and return as an array. Returns null if the setting was undefined or null.
   * @param state The current top-level redux state of the application.
   */
  selectorToArray = (state: S): string[] | null => {
    const value = this._value(state)?.toString().split(",");

    return value ?? null;
  };

  /**
   * Construct a new LocalSetting manager.
   * @param key The unique key of the setting.
   * @param innerSelector A selector which retrieves the LocalSettingsState from
   * the top-level redux state of the application.
   * @param defaultValue Optional default value of the setting when it has not
   * yet been set.
   */
  constructor(
    public key: string,
    innerSelector: Selector<S, LocalSettingsState>,
    defaultValue?: T,
  ) {
    this._value = createSelector(
      innerSelector,
      () => getValueFromSessionStorage(this.key),
      (uiSettings, cachedValue) => {
        if (cachedValue != null && uiSettings[this.key] == null) {
          uiSettings[this.key] = cachedValue;
        }
        return uiSettings[this.key] ?? cachedValue ?? defaultValue;
      },
    );
  }
}

export function* persistLocalSetting(action: PayloadAction<LocalSettingData>) {
  yield call(saveToSessionStorage, action.payload);
}

export function* localSettingsSaga() {
  yield takeEvery(SET_UI_VALUE, persistLocalSetting);
}
