import _ from "lodash";
import { Action, Dispatch } from "redux";
import * as protobuf from "protobufjs/minimal";

import * as protos from  "../js/protos";
import { PayloadAction } from "../interfaces/action";
import { getUIData, setUIData } from "../util/api";
import { AdminUIState } from "./state";

export const SET = "cockroachui/uidata/SET_OPTIN";
export const LOAD_ERROR = "cockroachui/uidata/LOAD_ERROR";
export const SAVE_ERROR = "cockroachui/uidata/SAVE_ERROR";
export const LOAD = "cockroachui/uidata/LOAD";
export const LOAD_COMPLETE = "cockroachui/uidata/LOAD_COMPLETE";
export const SAVE = "cockroachui/uidata/SAVE";
export const SAVE_COMPLETE = "cockroachui/uidata/SAVE_COMPLETE";

// Opt In Attribute Keys
export const KEY_HELPUS: string = "helpus";
// The "server." prefix denotes that this key is shared with the server, so
// changes to this key must be synchronized with the server code.
export const KEY_OPTIN: string = "server.optin-reporting";
// Tracks whether the latest registration data has been synchronized with the
// Cockroach Labs servers.
export const KEY_REGISTRATION_SYNCHRONIZED = "registration_synchronized";

/**
 * OptInAttributes tracks the values the user has provided when opting in to usage reporting
 */
export class OptInAttributes {
  email: string = "";
  optin: boolean = null; // Did the user opt in/out of reporting usage
  /**
   * Number of times the user has dismissed the opt-in banner. This was made a
   * number instead of a boolean for a feature that was not implemented, and is
   * currently only ever set to null or 1.
   */
  dismissed: number = null;
  firstname: string = "";
  lastname: string = "";
  company: string = "";
  updates: boolean = null; // Did the user sign up for product/feature updates
}

// VERSION_DISMISSED_KEY is the uiData key on the server that tracks when the outdated banner was last dismissed.
export const VERSION_DISMISSED_KEY = "version_dismissed";

export enum UIDataState {
  UNINITIALIZED, // Data has not been loaded yet.
  LOADING,
  LOADING_LOAD_ERROR, // Loading with an existing load error
  SAVING,
  SAVE_ERROR,
  LOAD_ERROR,
  VALID, // Data isn't loading/saving and has been successfully loaded/saved.
}

export class UIData {
  state: UIDataState = UIDataState.UNINITIALIZED;
  error: Error;
  data: any;
}

/**
 * UIDataSet maintains the current values of fields that are persisted to the
 * server as UIData. Fields are maintained in this collection as untyped
 * objects.
 */
export class UIDataSet {
  [key: string]: UIData;
}

/**
 * Reducer which modifies a UIDataSet.
 */
export default function (state = new UIDataSet(), action: Action): UIDataSet {
  if (_.isNil(action)) {
    return state;
  }

  switch (action.type) {
    case SET: {
      const { key, value } = (action as PayloadAction<KeyValue>).payload;
      state = _.clone(state);
      state[key] = _.clone(state[key]) || new UIData();
      state[key].state = UIDataState.VALID;
      state[key].data = value;
      state[key].error = null;
      return state;
    }
    case SAVE: {
      const keys = (action as PayloadAction<string[]>).payload;
      state = _.clone(state);
      _.each(keys, (k) => {
        state[k] = _.clone(state[k]) || new UIData();
        state[k].state = UIDataState.SAVING;
      });
      return state;
    }
    case SAVE_ERROR: {
      // TODO(tamird): https://github.com/palantir/tslint/issues/2551
      //
      // tslint:disable-next-line:no-use-before-declare
      const { key: saveErrorKey, error: saveError } = (action as PayloadAction<KeyedError>).payload;
      state = _.clone(state);
      state[saveErrorKey] = _.clone(state[saveErrorKey]) || new UIData();
      state[saveErrorKey].state = UIDataState.SAVE_ERROR;
      state[saveErrorKey].error = saveError;
      return state;
    }
    case LOAD: {
      const keys = (action as PayloadAction<string[]>).payload;
      state = _.clone(state);
      _.each(keys, (k) => {
        state[k] = _.clone(state[k]) || new UIData();
        state[k].state = UIDataState.LOADING;
      });
      return state;
    }
    case LOAD_ERROR: {
      // TODO(tamird): https://github.com/palantir/tslint/issues/2551
      //
      // tslint:disable-next-line:no-use-before-declare
      const { key: loadErrorKey, error: loadError } = (action as PayloadAction<KeyedError>).payload;
      state = _.clone(state);
      state[loadErrorKey] = _.clone(state[loadErrorKey]) || new UIData();
      state[loadErrorKey].state = UIDataState.LOAD_ERROR;
      state[loadErrorKey].error = loadError;
      return state;
    }
    default:
      return state;
  }
}

/**
 * setUIDataKey sets the value of the given UIData key.
 */
export function setUIDataKey(key: string, value: Object): PayloadAction<KeyValue> {
  return {
    type: SET,
    payload: { key, value },
  };
}

/**
 * errorUIData occurs when an asynchronous function related to UIData encounters
 * an error.
 */
export function loadErrorUIData(key: string, error: Error): PayloadAction<KeyedError> {
  return {
    type: LOAD_ERROR,
    payload: { key, error },
  };
}

/**
 * errorUIData occurs when an asynchronous function related to UIData encounters
 * an error.
 */
export function saveErrorUIData(key: string, error: Error): PayloadAction<KeyedError> {
  return {
    type: SAVE_ERROR,
    payload: { key, error },
  };
}

/**
 * loadUIData occurs when an asynchronous request to load UIData begins.
 */
export function beginLoadUIData(keys: string[]): PayloadAction<string[]> {
  return {
    type: LOAD,
    payload: keys,
  };
}

/**
 * saveUIData occurs when an asynchronous request for UIData begins.
 */
export function beginSaveUIData(keys: string[]): PayloadAction<string[]> {
  return {
    type: SAVE,
    payload: keys,
  };
}

/**
 * A generic KeyValue type used for convenience when calling saveUIData.
 */
export interface KeyValue {
  key: string;
  value: Object;
}

/**
 * KeyedError associates an error with a key to use as an action payload.
 */
export interface KeyedError {
  key: string;
  error: Error;
}

// HELPER FUNCTIONS

// Returns true if the key exists and the data is valid.
export function isValid(state: AdminUIState, key: string) {
  return state.uiData[key] && (state.uiData[key].state === UIDataState.VALID) || false;
}

// Returns contents of the data field if the key is valid, undefined otherwise.
export function getData(state: AdminUIState, key: string) {
  return isValid(state, key) ? state.uiData[key].data : undefined;
}

// Returns true if the given key exists and is in the SAVING state.
export function isSaving(state: AdminUIState, key: string) {
  return state.uiData[key] && (state.uiData[key].state === UIDataState.SAVING) || false;
}

// Returns true if the given key exists and is in the SAVING state.
export function isLoading(state: AdminUIState, key: string) {
  return state.uiData[key] && (state.uiData[key].state === UIDataState.LOADING) || false;
}

// Returns true if the key exists and is in either the SAVING or LOADING state.
export function isInFlight(state: AdminUIState, key: string) {
  return state.uiData[key] && ((state.uiData[key].state === UIDataState.SAVING) || (state.uiData[key].state === UIDataState.LOADING)) || false;
}

// Returns the error field if the key exists and is in the SAVE_ERROR state.
// Returns null otherwise.
export function getSaveError(state: AdminUIState, key: string): Error {
  return (state.uiData[key] && (state.uiData[key].state === UIDataState.SAVE_ERROR || state.uiData[key].state === UIDataState.SAVING)) ? state.uiData[key].error : null;
}

// Returns the error field if the key exists and is in the LOAD_ERROR state.
// Returns null otherwise.
export function getLoadError(state: AdminUIState, key: string): Error {
  return (state.uiData[key] && (state.uiData[key].state === UIDataState.LOAD_ERROR || state.uiData[key].state === UIDataState.LOADING)) ? state.uiData[key].error : null;
}

/**
 * saveUIData saves the value one (or more) UIData objects to the server. After
 * the values have been successfully persisted to the server, they are updated
 * in the local UIDataSet store.
 */
export function saveUIData(...values: KeyValue[]) {
  return (dispatch: Dispatch<AdminUIState>, getState: () => AdminUIState): Promise<void> => {
    let state = getState();
    values = _.filter(values, (kv) => !isInFlight(state, kv.key));
    if (values.length === 0) {
      return;
    }
    dispatch(beginSaveUIData(_.map(values, (kv) => kv.key)));

    // Encode data for each UIData key.
    let request = new protos.cockroach.server.serverpb.SetUIDataRequest();
    _.each(values, (kv) => {
      const stringifiedValue = JSON.stringify(kv.value);
      const buffer = new Uint8Array(protobuf.util.utf8.length(stringifiedValue));
      protobuf.util.utf8.write(stringifiedValue, buffer, 0);
      request.key_values[kv.key] = buffer;
    });

    return setUIData(request).then((_response) => {
      // SetUIDataResponse is empty. A positive return indicates success.
      _.each(values, (kv) => dispatch(setUIDataKey(kv.key, kv.value)));
    }).catch((error) => {
      // TODO(maxlang): Fix error handling more comprehensively.
      // Tracked in #8699
      setTimeout(() => _.each(values, (kv) => dispatch(saveErrorUIData(kv.key, error))), 1000);
    });
  };
}

/**
 * loadUIData loads the values of the give UIData keys from the server.
 */
export function loadUIData(...keys: string[]) {
  return (dispatch: Dispatch<AdminUIState>, getState: () => AdminUIState): Promise<void> => {
    let state = getState();
    keys = _.filter(keys, (k) => !isInFlight(state, k));
    if (keys.length === 0) {
      return;
    }
    dispatch(beginLoadUIData(keys));

    return getUIData(new protos.cockroach.server.serverpb.GetUIDataRequest({ keys })).then((response) => {
      // Decode data for each UIData key.
      _.each(keys, (key) => {
        if (_.has(response.key_values, key)) {
          const buffer = response.key_values[key].value;
          dispatch(setUIDataKey(key, JSON.parse(protobuf.util.utf8.read(buffer, 0, buffer.byteLength))));
        } else {
          dispatch(setUIDataKey(key, undefined));
        }
      });
    }).catch((error) => {
      // TODO(maxlang): Fix error handling more comprehensively.
      // Tracked in #8699
      setTimeout(() => _.each(keys, (key) => dispatch(loadErrorUIData(key, error))), 1000);
    });
  };
}
