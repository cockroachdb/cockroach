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

import _ from "lodash";
import { createSelector, Selector } from "reselect";
import { Action } from "redux";
import { PayloadAction } from "src/interfaces/action";

const SET_UI_VALUE = "cockroachui/ui/SET_UI_VALUE";

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
 * reducer function which handles local settings, storing them in a dictionary.
 */
export function localSettingsReducer(state: LocalSettingsState = {}, action: Action): LocalSettingsState {
  if (_.isNil(action)) {
    return state;
  }

  switch (action.type) {
    case SET_UI_VALUE:
      const { payload } = action as PayloadAction<LocalSettingData>;
      state = _.clone(state);
      state[payload.key] = payload.value;
      return state;
    default:
      return state;
  }
}

/**
 * Action creator to set a named local setting.
 */
export function setLocalSetting(key: string, value: any): PayloadAction<LocalSettingData> {
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
  }

  /**
   * Selector which retrieves this setting from the LocalSettingsState
   * @param state The current top-level redux state of the application.
   */
  selector = (state: S) => {
    return this._value(state);
  }

  /**
   * Construct a new LocalSetting manager.
   * @param key The unique key of the setting.
   * @param innerSelector A selector which retrieves the LocalSettingsState from
   * the top-level redux state of the application.
   * @param defaultValue Optional default value of the setting when it has not
   * yet been set.
   */
  constructor(public key: string, innerSelector: Selector<S, LocalSettingsState>, defaultValue?: T) {
    this._value = createSelector(
      innerSelector,
      (uiSettings) => {
        return uiSettings[this.key] || defaultValue;
      },
    );
  }
}
