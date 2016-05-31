import _ = require("lodash");

import { Dispatch } from "redux";
import ByteBuffer = require("bytebuffer");

import * as protos from  "../js/protos";
import { Action, PayloadAction } from "../interfaces/action";
import { getUIData, setUIData } from "../util/api";

export const SET = "cockroachui/uidata/SET_OPTIN";
export const ERROR = "cockroachui/uidata/ERROR";
export const FETCH = "cockroachui/uidata/FETCH";
export const FETCH_COMPLETE = "cockroachui/uidata/FETCH_COMPLETE";

/**
 * UIDataSet maintains the current values of fields that are persisted to the
 * server as UIData. Fields are maintained in this collection as untyped
 * objects.
 */
export class UIDataSet {
  inFlight = 0;
  error: Error;
  data: {[key: string]: any} = {};
}

/**
 * Reducer which modifies a UIDataSet.
 */
export default function(state = new UIDataSet(), action: Action): UIDataSet {
  switch (action.type) {
    case SET:
      let {key, value} = (action as PayloadAction<KeyValue>).payload;
      state = _.clone(state);
      state.data[key] = value;
      state.error = null;
      return state;
    case ERROR:
      let { payload } = action as PayloadAction<Error>;
      state = _.clone(state);
      state.error = payload;
      return state;
    case FETCH:
      state = _.clone(state);
      state.inFlight++;
      return state;
    case FETCH_COMPLETE:
      state = _.clone(state);
      state.inFlight--;
      return state;
    default:
      return state;
  }
}

/**
 * setUIDataKey sets the value of the given UIData key.
 */
export function setUIDataKey(key: string, value: any): PayloadAction<KeyValue> {
  return {
    type: SET,
    payload: { key, value },
  };
}

/**
 * errorUIData occurs when an asynchronous function related to UIData encounters
 * an error.
 */
export function errorUIData(err: Error): PayloadAction<Error> {
  return {
    type: ERROR,
    payload: err,
  };
}

/**
 * fetchUIData occurs when an asynchronous request for UIData begins.
 */
export function fetchUIData(): Action {
  return {
    type: FETCH,
  };
}

/**
 * fetchCompleteUIData occurs when an asynchronous request for UIData completes.
 */
export function fetchCompleteUIData(): Action {
  return {
    type: FETCH_COMPLETE,
  };
}

/**
 * A generic KeyValue type used for convenience when calling saveUIData.
 */
export interface KeyValue {
  key: string;
  value: any;
}

/**
 * saveUIData saves the value one (or more) UIData objects to the server. After
 * the values have been successfully persisted to the server, they are updated
 * in the local UIDataSet store.
 */
export function saveUIData(...values: KeyValue[]) {
  return (dispatch: Dispatch, getState: () => any): Promise<void> => {
    dispatch(fetchUIData());

    // Encode data for each UIData key. Each object is stringified and written
    // to a ByteBuffer.
    let request = new protos.cockroach.server.SetUIDataRequest();
    _.each(values, (kv) => {
      let stringifiedValue = JSON.stringify(kv.value);
      request.key_values.set(kv.key, ByteBuffer.fromUTF8(stringifiedValue));
    });

    return setUIData(request).then((response) => {
      // SetUIDataResponse is empty. A positive return indicates success.
      _.each(values, (kv) => dispatch(setUIDataKey(kv.key, kv.value)));
    }).catch((error) => {
      dispatch(errorUIData(error));
    }).then(() => {
      // Runs in all cases.
      dispatch(fetchCompleteUIData());
    });
  };
}

/**
 * loadUIData loads the values of the give UIData keys from the server.
 */
export function loadUIData(...keys: string[]) {
  return (dispatch: Dispatch, getState: () => any): Promise<void> => {
    dispatch(fetchUIData());

    return getUIData({ keys }).then((response) => {
      response.getKeyValues().forEach((val, key) => {
        // Responses from the server return values as ByteBuffer objects, which
        // represent stringified JSON objects.
        let decoded: any = null;
        let bb = val.getValue();
        let str = bb.readString(bb.limit);
        if (str) {
          decoded = JSON.parse(str);
        }
        dispatch(setUIDataKey(key, decoded));
      });
    }).catch((error) => {
      dispatch(errorUIData(error));
    }).then(() => {
      // Runs in all cases.
      dispatch(fetchCompleteUIData());
    });
  };
}
