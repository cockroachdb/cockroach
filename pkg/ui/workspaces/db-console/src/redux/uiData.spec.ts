// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import keys from "lodash/keys";
import * as protobuf from "protobufjs/minimal";
import { Action } from "redux";

import * as protos from "src/js/protos";
import * as api from "src/util/api";
import fetchMock from "src/util/fetch-mock";

import * as uidata from "./uiData";

describe("UIData reducer", function () {
  describe("actions", function () {
    it("setUIDataKey() creates the correct action type.", function () {
      expect(uidata.setUIDataKey("string", null).type).toEqual(uidata.SET);
    });

    it("beginSaveUIData() creates the correct action type.", function () {
      expect(uidata.beginSaveUIData([]).type).toEqual(uidata.SAVE);
    });

    it("saveErrorUIData() creates the correct action type.", function () {
      expect(uidata.saveErrorUIData(null, null).type).toEqual(
        uidata.SAVE_ERROR,
      );
    });

    it("beginLoadUIData() creates the correct action type.", function () {
      expect(uidata.beginLoadUIData([]).type).toEqual(uidata.LOAD);
    });

    it("loadErrorUIData() creates the correct action type.", function () {
      expect(uidata.loadErrorUIData(null, null).type).toEqual(
        uidata.LOAD_ERROR,
      );
    });
  });

  describe("helper functions", function () {
    let state: any;

    beforeEach(function () {
      state = { uiData: uidata.uiDataReducer(undefined, { type: "unknown" }) };
    });

    const dispatch = (action: Action) => {
      state = { uiData: uidata.uiDataReducer(state.uiData, action) };
    };

    it("isValid", function () {
      const key1 = "key1";
      const key2 = "key2";

      expect(uidata.isValid(state, key1)).toBe(false);
      expect(uidata.isValid(state, key2)).toBe(false);

      dispatch(uidata.setUIDataKey(key1, null));

      expect(uidata.isValid(state, key1)).toBeTruthy();
      expect(uidata.isValid(state, key2)).toBe(false);

      dispatch(uidata.setUIDataKey(key2, null));

      expect(uidata.isValid(state, key1)).toBeTruthy();
      expect(uidata.isValid(state, key2)).toBeTruthy();
    });

    it("getData", function () {
      const key1 = "key1";
      const key2 = "key2";
      const value1 = "value1";
      const value2 = "value2";

      expect(uidata.getData(state, key1)).toBeUndefined();
      expect(uidata.getData(state, key2)).toBeUndefined();

      dispatch(uidata.setUIDataKey(key1, value1));

      expect(uidata.getData(state, key1)).toEqual(value1);
      expect(uidata.getData(state, key2)).toBeUndefined();

      dispatch(uidata.setUIDataKey(key2, value2));

      expect(uidata.getData(state, key1)).toEqual(value1);
      expect(uidata.getData(state, key2)).toEqual(value2);
    });

    it("isSaving and isInFlight", function () {
      const key1 = "key1";
      const key2 = "key2";
      const saving = (k: string) => uidata.isSaving(state, k);
      const inFlight = (k: string) => uidata.isInFlight(state, k);

      expect(saving(key1)).toBe(false);
      expect(inFlight(key1)).toBe(false);
      expect(saving(key2)).toBe(false);
      expect(inFlight(key2)).toBe(false);

      dispatch(uidata.beginSaveUIData([key1]));

      expect(saving(key1)).toBeTruthy();
      expect(inFlight(key1)).toBeTruthy();
      expect(saving(key2)).toBe(false);
      expect(inFlight(key2)).toBe(false);

      dispatch(uidata.beginSaveUIData([key1, key2]));

      expect(saving(key1)).toBeTruthy();
      expect(inFlight(key1)).toBeTruthy();
      expect(saving(key2)).toBeTruthy();
      expect(inFlight(key2)).toBeTruthy();

      dispatch(uidata.beginLoadUIData([key1, key2]));

      expect(saving(key1)).toBe(false);
      expect(inFlight(key1)).toBeTruthy();
      expect(saving(key2)).toBe(false);
      expect(inFlight(key2)).toBeTruthy();

      dispatch(uidata.setUIDataKey(key2, null));

      expect(saving(key1)).toBe(false);
      expect(inFlight(key1)).toBeTruthy();
      expect(saving(key2)).toBe(false);
      expect(inFlight(key2)).toBe(false);
    });

    it("getSaveError and getLoadError", function () {
      const key1 = "key1";
      const key2 = "key2";
      const saveError = (k: string) => uidata.getSaveError(state, k);
      const loadError = (k: string) => uidata.getLoadError(state, k);

      expect(saveError(key1)).toBeNull();
      expect(saveError(key2)).toBeNull();
      expect(loadError(key1)).toBeNull();
      expect(loadError(key2)).toBeNull();

      let e = new Error();
      dispatch(uidata.saveErrorUIData(key1, e));

      expect(saveError(key1)).toEqual(e);
      expect(saveError(key2)).toBeNull();
      expect(loadError(key1)).toBeNull();
      expect(loadError(key2)).toBeNull();

      e = new Error();
      dispatch(uidata.loadErrorUIData(key1, e));

      expect(saveError(key1)).toBeNull();
      expect(saveError(key2)).toBeNull();
      expect(loadError(key1)).toEqual(e);
      expect(loadError(key2)).toBeNull();

      dispatch(uidata.beginLoadUIData([key1]));

      expect(saveError(key1)).toBeNull();
      expect(saveError(key2)).toBeNull();
      expect(loadError(key1)).toEqual(e);
      expect(loadError(key2)).toBeNull();

      let e2 = new Error();
      dispatch(uidata.saveErrorUIData(key2, e2));

      expect(saveError(key1)).toBeNull();
      expect(saveError(key2)).toEqual(e2);
      expect(loadError(key1)).toEqual(e);
      expect(loadError(key2)).toBeNull();

      e2 = new Error();
      dispatch(uidata.loadErrorUIData(key2, e2));

      expect(saveError(key1)).toBeNull();
      expect(saveError(key2)).toBeNull();
      expect(loadError(key1)).toEqual(e);
      expect(loadError(key2)).toEqual(e2);

      dispatch(uidata.beginLoadUIData([key2]));

      expect(saveError(key1)).toBeNull();
      expect(saveError(key2)).toBeNull();
      expect(loadError(key1)).toEqual(e);
      expect(loadError(key2)).toEqual(e2);

      dispatch(uidata.setUIDataKey(key1, null));

      expect(saveError(key1)).toBeNull();
      expect(saveError(key2)).toBeNull();
      expect(loadError(key1)).toBeNull();
      expect(loadError(key2)).toEqual(e2);

      dispatch(uidata.setUIDataKey(key2, null));

      expect(saveError(key1)).toBeNull();
      expect(saveError(key2)).toBeNull();
      expect(loadError(key1)).toBeNull();
      expect(loadError(key2)).toBeNull();
    });
  });

  describe("reducer", function () {
    let state: uidata.UIDataState;

    beforeEach(function () {
      state = uidata.uiDataReducer(undefined, { type: "unknown" });
    });

    const dispatch = (action: Action) => {
      state = uidata.uiDataReducer(state, action);
    };

    it("should have the correct default value.", function () {
      const expected = {};
      expect(state).toEqual(expected);
    });

    it("should correctly dispatch setUIDataKey.", function () {
      const objKey = "obj";
      const boolKey = "bool";
      const numKey = "num";
      const obj = { value: 1 };
      const bool = true;
      const num = 240;

      expect(state[objKey]).toBeUndefined();
      expect(state[boolKey]).toBeUndefined();
      expect(state[numKey]).toBeUndefined();

      // Validate setting a variety of object types.
      dispatch(uidata.setUIDataKey(objKey, obj));
      dispatch(uidata.setUIDataKey(boolKey, bool));
      dispatch(uidata.setUIDataKey(numKey, num));

      expect(keys(state).length).toBe(3);
      expect(state[objKey].data).toEqual(obj);
      expect(state[objKey].status).toEqual(uidata.UIDataStatus.VALID);
      expect(state[boolKey].data).toEqual(bool);
      expect(state[boolKey].status).toEqual(uidata.UIDataStatus.VALID);
      expect(state[numKey].data).toEqual(num);
      expect(state[numKey].status).toEqual(uidata.UIDataStatus.VALID);

      // validate overwrite.
      const obj2 = { value: 2 };
      dispatch(uidata.setUIDataKey(objKey, obj2));
      expect(keys(state).length).toBe(3);
      expect(state[objKey].data).toEqual(obj2);
      expect(state[objKey].status).toEqual(uidata.UIDataStatus.VALID);
    });

    it("should correctly dispatch loadErrorUIData.", function () {
      const key1 = "key1";
      const err = new Error("an error.");
      expect(state[key1]).toBeUndefined();
      dispatch(uidata.loadErrorUIData(key1, err));
      expect(state[key1].status).toEqual(uidata.UIDataStatus.LOAD_ERROR);
      expect(state[key1].error).toEqual(err);

      dispatch(uidata.setUIDataKey(key1, 4));
      expect(state[key1].status).toEqual(uidata.UIDataStatus.VALID);
      expect(state[key1].error).toBeNull();
    });

    it("should correctly dispatch saveErrorUIData.", function () {
      const key1 = "key1";
      const err = new Error("an error.");
      expect(state[key1]).toBeUndefined();
      dispatch(uidata.saveErrorUIData(key1, err));
      expect(state[key1].status).toEqual(uidata.UIDataStatus.SAVE_ERROR);
      expect(state[key1].error).toEqual(err);

      dispatch(uidata.setUIDataKey(key1, 4));
      expect(state[key1].status).toEqual(uidata.UIDataStatus.VALID);
      expect(state[key1].error).toBeNull();
    });

    it("should correctly dispatch beginSaveUIData", function () {
      const key1 = "key1";
      const key2 = "key2";
      const keysArr = [key1, key2];
      dispatch(uidata.beginSaveUIData(keysArr));
      expect(keys(state).length).toBe(2);
      expect(state[key1].status).toEqual(uidata.UIDataStatus.SAVING);
      expect(state[key2].status).toEqual(uidata.UIDataStatus.SAVING);
      dispatch(uidata.setUIDataKey(key1, "value1"));
      dispatch(uidata.setUIDataKey(key2, "value2"));
      expect(state[key1].status).toEqual(uidata.UIDataStatus.VALID);
      expect(state[key2].status).toEqual(uidata.UIDataStatus.VALID);
    });

    it("should correctly dispatch beginLoadUIData", function () {
      const key1 = "key1";
      const key2 = "key2";
      const keysArr = [key1, key2];
      dispatch(uidata.beginLoadUIData(keysArr));
      expect(keys(state).length).toBe(2);
      expect(state[key1].status).toEqual(uidata.UIDataStatus.LOADING);
      expect(state[key2].status).toEqual(uidata.UIDataStatus.LOADING);
      dispatch(uidata.setUIDataKey(key1, "value1"));
      dispatch(uidata.setUIDataKey(key2, "value2"));
      expect(state[key1].status).toEqual(uidata.UIDataStatus.VALID);
      expect(state[key2].status).toEqual(uidata.UIDataStatus.VALID);
    });
  });

  describe("asynchronous actions", function () {
    let state: uidata.UIDataState;

    const dispatch = (action: Action) => {
      state = uidata.uiDataReducer(state, action);
    };

    const uiKey1 = "a_key";
    const uiObj1 = {
      setting1: "value",
      setting2: true,
    };

    const uiKey2 = "another_key";
    const uiObj2 = 1234;

    const saveUIData = function (...values: uidata.KeyValue[]): Promise<void> {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      return uidata.saveUIData.apply(this, values)(dispatch, () => {
        return { uiData: state };
      });
    };

    const loadUIData = function (...keys: string[]): Promise<void> {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      return uidata.loadUIData.apply(this, keys)(dispatch, () => {
        return { uiData: state };
      });
    };

    beforeEach(function () {
      state = uidata.uiDataReducer(undefined, { type: "unknown" });
    });

    afterEach(fetchMock.restore);

    it("correctly saves UIData", function () {
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/uidata`,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(state[uiKey1].status).toEqual(uidata.UIDataStatus.SAVING);
          expect(state[uiKey2].status).toEqual(uidata.UIDataStatus.SAVING);

          const kvs = protos.cockroach.server.serverpb.SetUIDataRequest.decode(
            new Uint8Array(requestObj.body as ArrayBuffer),
          ).key_values;

          expect(keys(kvs).length).toBe(2);

          const deserialize = function (buffer: Uint8Array): Object {
            return JSON.parse(
              protobuf.util.utf8.read(buffer, 0, buffer.byteLength),
            );
          };

          expect(deserialize(kvs[uiKey1])).toEqual(uiObj1);
          expect(deserialize(kvs[uiKey2])).toEqual(uiObj2);

          const encodedResponse =
            protos.cockroach.server.serverpb.SetUIDataResponse.encode(
              {},
            ).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      const p = saveUIData(
        { key: uiKey1, value: uiObj1 },
        { key: uiKey2, value: uiObj2 },
      );

      // Second save should be ignored.
      const p2 = saveUIData(
        { key: uiKey1, value: uiObj1 },
        { key: uiKey2, value: uiObj2 },
      );

      return Promise.all([p, p2]).then(() => {
        expect(fetchMock.calls(`${api.API_PREFIX}/uidata`).length).toBe(1);
        expect(keys(state).length).toBe(2);
        expect(state[uiKey1].data).toEqual(uiObj1);
        expect(state[uiKey2].data).toEqual(uiObj2);
        expect(state[uiKey1].error).toBeNull();
        expect(state[uiKey2].error).toBeNull();
        expect(state[uiKey1].status).toEqual(uidata.UIDataStatus.VALID);
        expect(state[uiKey2].status).toEqual(uidata.UIDataStatus.VALID);
      });
    });

    it("correctly reacts to error during save", function (done) {
      jest.setTimeout(2000);
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/uidata`,
        method: "POST",
        response: () => {
          return { throws: new Error(), status: 500 };
        },
      });

      const p = saveUIData(
        { key: uiKey1, value: uiObj1 },
        { key: uiKey2, value: uiObj2 },
      );

      p.then(() => {
        expect(fetchMock.calls(`${api.API_PREFIX}/uidata`).length).toBe(1);
        expect(keys(state).length).toBe(2);
        expect(state[uiKey1].status).toEqual(uidata.UIDataStatus.SAVING);
        expect(state[uiKey2].status).toEqual(uidata.UIDataStatus.SAVING);
        expect(state[uiKey1].data).toBeUndefined();
        expect(state[uiKey2].data).toBeUndefined();
        expect("data" in state[uiKey1]).toBeFalsy();
        expect("data" in state[uiKey2]).toBeFalsy();
        expect(state[uiKey1].error).toBeUndefined();
        expect(state[uiKey2].error).toBeUndefined();
        setTimeout(() => {
          expect(state[uiKey1].status).toEqual(uidata.UIDataStatus.SAVE_ERROR);
          expect(state[uiKey2].status).toEqual(uidata.UIDataStatus.SAVE_ERROR);
          expect(state[uiKey1].error).toBeInstanceOf(Error);
          expect(state[uiKey2].error).toBeInstanceOf(Error);
          done();
        }, 1000);
      });
    });

    it("correctly loads UIData", function () {
      const expectedURL = `${api.API_PREFIX}/uidata?keys=${uiKey1}&keys=${uiKey2}`;

      fetchMock.mock({
        matcher: expectedURL,
        method: "GET",
        response: () => {
          // FetchMock URL must match the above string exactly, requesting both
          // keys.
          expect(state[uiKey1].status).toEqual(uidata.UIDataStatus.LOADING);
          expect(state[uiKey2].status).toEqual(uidata.UIDataStatus.LOADING);

          const response: protos.cockroach.server.serverpb.IGetUIDataResponse =
            {
              key_values: {},
            };
          const setValue = function (key: string, obj: Object) {
            const stringifiedValue = JSON.stringify(obj);
            const buffer = new Uint8Array(
              protobuf.util.utf8.length(stringifiedValue),
            );
            protobuf.util.utf8.write(stringifiedValue, buffer, 0);
            response.key_values[key] = { value: buffer };
          };
          setValue(uiKey1, uiObj1);
          setValue(uiKey2, uiObj2);

          const encodedResponse =
            protos.cockroach.server.serverpb.GetUIDataResponse.encode(
              response,
            ).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      const p = loadUIData(uiKey1, uiKey2);
      const p2 = loadUIData(uiKey1, uiKey2); // Second load should be ignored.

      return Promise.all([p, p2]).then(() => {
        expect(fetchMock.calls(expectedURL).length).toBe(1);
        expect(keys(state).length).toBe(2);
        expect(state[uiKey1].data).toEqual(uiObj1);
        expect(state[uiKey2].data).toEqual(uiObj2);
        expect(state[uiKey1].error).toBeNull();
        expect(state[uiKey2].error).toBeNull();
        expect(state[uiKey1].status).toEqual(uidata.UIDataStatus.VALID);
        expect(state[uiKey2].status).toEqual(uidata.UIDataStatus.VALID);
      });
    });

    it("correctly reacts to error during load", function (done) {
      jest.setTimeout(2000);

      const uidataPrefixMatcher = `begin:${api.API_PREFIX}/uidata`;

      fetchMock.mock({
        matcher: uidataPrefixMatcher,
        response: () => {
          return { throws: new Error() };
        },
      });

      const p = loadUIData(uiKey1, uiKey2);

      p.then(() => {
        expect(fetchMock.calls(uidataPrefixMatcher).length).toBe(1);
        expect(keys(state).length).toBe(2);
        expect(state[uiKey1].status).toEqual(uidata.UIDataStatus.LOADING);
        expect(state[uiKey2].status).toEqual(uidata.UIDataStatus.LOADING);
        expect(state[uiKey1].data).toBeUndefined();
        expect(state[uiKey2].data).toBeUndefined();
        expect("data" in state[uiKey1]).toBeFalsy();
        expect("data" in state[uiKey2]).toBeFalsy();
        expect(state[uiKey1].error).toBeUndefined();
        expect(state[uiKey2].error).toBeUndefined();
        setTimeout(() => {
          expect(state[uiKey1].status).toEqual(uidata.UIDataStatus.LOAD_ERROR);
          expect(state[uiKey2].status).toEqual(uidata.UIDataStatus.LOAD_ERROR);
          expect(state[uiKey1].error).toBeInstanceOf(Error);
          expect(state[uiKey2].error).toBeInstanceOf(Error);
          done();
        }, 1000);
      });
    });

    it("handles missing keys", function () {
      const missingKey = "missingKey";

      const expectedURL = `${api.API_PREFIX}/uidata?keys=${missingKey}`;

      fetchMock.mock({
        matcher: expectedURL,
        response: () => {
          expect(state[missingKey].status).toEqual(uidata.UIDataStatus.LOADING);

          const encodedResponse =
            protos.cockroach.server.serverpb.GetUIDataResponse.encode(
              {},
            ).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      const p = loadUIData(missingKey);

      return p.then(() => {
        expect(fetchMock.calls(expectedURL).length).toBe(1);
        expect(keys(state).length).toBe(1);
        expect(state[missingKey].data).toEqual(undefined);
        expect("data" in state[missingKey]).toBeTruthy();
        expect(state[missingKey].status).toEqual(uidata.UIDataStatus.VALID);
        expect(state[missingKey].error).toBeNull();
      });
    });
  });
});
