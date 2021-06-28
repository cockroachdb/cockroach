// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { assert } from "chai";
import _ from "lodash";
import { Action } from "redux";
import * as protobuf from "protobufjs/minimal";
import fetchMock from "src/util/fetch-mock";

import * as protos from "src/js/protos";
import * as api from "src/util/api";
import * as uidata from "./uiData";

describe("UIData reducer", function () {
  describe("actions", function () {
    it("setUIDataKey() creates the correct action type.", function () {
      assert.equal(uidata.setUIDataKey("string", null).type, uidata.SET);
    });

    it("beginSaveUIData() creates the correct action type.", function () {
      assert.equal(uidata.beginSaveUIData([]).type, uidata.SAVE);
    });

    it("saveErrorUIData() creates the correct action type.", function () {
      assert.equal(uidata.saveErrorUIData(null, null).type, uidata.SAVE_ERROR);
    });

    it("beginLoadUIData() creates the correct action type.", function () {
      assert.equal(uidata.beginLoadUIData([]).type, uidata.LOAD);
    });

    it("loadErrorUIData() creates the correct action type.", function () {
      assert.equal(uidata.loadErrorUIData(null, null).type, uidata.LOAD_ERROR);
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

      assert.isFalse(uidata.isValid(state, key1));
      assert.isFalse(uidata.isValid(state, key2));

      dispatch(uidata.setUIDataKey(key1, null));

      assert(uidata.isValid(state, key1));
      assert.isFalse(uidata.isValid(state, key2));

      dispatch(uidata.setUIDataKey(key2, null));

      assert(uidata.isValid(state, key1));
      assert(uidata.isValid(state, key2));
    });

    it("getData", function () {
      const key1 = "key1";
      const key2 = "key2";
      const value1 = "value1";
      const value2 = "value2";

      assert.isUndefined(uidata.getData(state, key1));
      assert.isUndefined(uidata.getData(state, key2));

      dispatch(uidata.setUIDataKey(key1, value1));

      assert.equal(uidata.getData(state, key1), value1);
      assert.isUndefined(uidata.getData(state, key2));

      dispatch(uidata.setUIDataKey(key2, value2));

      assert.equal(uidata.getData(state, key1), value1);
      assert.equal(uidata.getData(state, key2), value2);
    });

    it("isSaving and isInFlight", function () {
      const key1 = "key1";
      const key2 = "key2";
      const saving = (k: string) => uidata.isSaving(state, k);
      const inFlight = (k: string) => uidata.isInFlight(state, k);

      assert.isFalse(saving(key1));
      assert.isFalse(inFlight(key1));
      assert.isFalse(saving(key2));
      assert.isFalse(inFlight(key2));

      dispatch(uidata.beginSaveUIData([key1]));

      assert(saving(key1));
      assert(inFlight(key1));
      assert.isFalse(saving(key2));
      assert.isFalse(inFlight(key2));

      dispatch(uidata.beginSaveUIData([key1, key2]));

      assert(saving(key1));
      assert(inFlight(key1));
      assert(saving(key2));
      assert(inFlight(key2));

      dispatch(uidata.beginLoadUIData([key1, key2]));

      assert.isFalse(saving(key1));
      assert(inFlight(key1));
      assert.isFalse(saving(key2));
      assert(inFlight(key2));

      dispatch(uidata.setUIDataKey(key2, null));

      assert.isFalse(saving(key1));
      assert(inFlight(key1));
      assert.isFalse(saving(key2));
      assert.isFalse(inFlight(key2));
    });

    it("getSaveError and getLoadError", function () {
      const key1 = "key1";
      const key2 = "key2";
      const saveError = (k: string) => uidata.getSaveError(state, k);
      const loadError = (k: string) => uidata.getLoadError(state, k);

      assert.isNull(saveError(key1));
      assert.isNull(saveError(key2));
      assert.isNull(loadError(key1));
      assert.isNull(loadError(key2));

      let e = new Error();
      dispatch(uidata.saveErrorUIData(key1, e));

      assert.equal(saveError(key1), e);
      assert.isNull(saveError(key2));
      assert.isNull(loadError(key1));
      assert.isNull(loadError(key2));

      e = new Error();
      dispatch(uidata.loadErrorUIData(key1, e));

      assert.isNull(saveError(key1));
      assert.isNull(saveError(key2));
      assert.equal(loadError(key1), e);
      assert.isNull(loadError(key2));

      dispatch(uidata.beginLoadUIData([key1]));

      assert.isNull(saveError(key1));
      assert.isNull(saveError(key2));
      assert.equal(loadError(key1), e);
      assert.isNull(loadError(key2));

      let e2 = new Error();
      dispatch(uidata.saveErrorUIData(key2, e2));

      assert.isNull(saveError(key1));
      assert.equal(saveError(key2), e2);
      assert.equal(loadError(key1), e);
      assert.isNull(loadError(key2));

      e2 = new Error();
      dispatch(uidata.loadErrorUIData(key2, e2));

      assert.isNull(saveError(key1));
      assert.isNull(saveError(key2));
      assert.equal(loadError(key1), e);
      assert.equal(loadError(key2), e2);

      dispatch(uidata.beginLoadUIData([key2]));

      assert.isNull(saveError(key1));
      assert.isNull(saveError(key2));
      assert.equal(loadError(key1), e);
      assert.equal(loadError(key2), e2);

      dispatch(uidata.setUIDataKey(key1, null));

      assert.isNull(saveError(key1));
      assert.isNull(saveError(key2));
      assert.isNull(loadError(key1));
      assert.equal(loadError(key2), e2);

      dispatch(uidata.setUIDataKey(key2, null));

      assert.isNull(saveError(key1));
      assert.isNull(saveError(key2));
      assert.isNull(loadError(key1));
      assert.isNull(loadError(key2));
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
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch setUIDataKey.", function () {
      const objKey = "obj";
      const boolKey = "bool";
      const numKey = "num";
      const obj = { value: 1 };
      const bool = true;
      const num = 240;

      assert.isUndefined(state[objKey]);
      assert.isUndefined(state[boolKey]);
      assert.isUndefined(state[numKey]);

      // Validate setting a variety of object types.
      dispatch(uidata.setUIDataKey(objKey, obj));
      dispatch(uidata.setUIDataKey(boolKey, bool));
      dispatch(uidata.setUIDataKey(numKey, num));

      assert.lengthOf(_.keys(state), 3);
      assert.equal(state[objKey].data, obj);
      assert.equal(state[objKey].status, uidata.UIDataStatus.VALID);
      assert.equal(state[boolKey].data, bool);
      assert.equal(state[boolKey].status, uidata.UIDataStatus.VALID);
      assert.equal(state[numKey].data, num);
      assert.equal(state[numKey].status, uidata.UIDataStatus.VALID);

      // validate overwrite.
      const obj2 = { value: 2 };
      dispatch(uidata.setUIDataKey(objKey, obj2));
      assert.lengthOf(_.keys(state), 3);
      assert.equal(state[objKey].data, obj2);
      assert.equal(state[objKey].status, uidata.UIDataStatus.VALID);
    });

    it("should correctly dispatch loadErrorUIData.", function () {
      const key1 = "key1";
      const err = new Error("an error.");
      assert.isUndefined(state[key1]);
      dispatch(uidata.loadErrorUIData(key1, err));
      assert.equal(state[key1].status, uidata.UIDataStatus.LOAD_ERROR);
      assert.equal(state[key1].error, err);

      dispatch(uidata.setUIDataKey(key1, 4));
      assert.equal(state[key1].status, uidata.UIDataStatus.VALID);
      assert.isNull(state[key1].error);
    });

    it("should correctly dispatch saveErrorUIData.", function () {
      const key1 = "key1";
      const err = new Error("an error.");
      assert.isUndefined(state[key1]);
      dispatch(uidata.saveErrorUIData(key1, err));
      assert.equal(state[key1].status, uidata.UIDataStatus.SAVE_ERROR);
      assert.equal(state[key1].error, err);

      dispatch(uidata.setUIDataKey(key1, 4));
      assert.equal(state[key1].status, uidata.UIDataStatus.VALID);
      assert.isNull(state[key1].error);
    });

    it("should correctly dispatch beginSaveUIData", function () {
      const key1 = "key1";
      const key2 = "key2";
      const keys = [key1, key2];
      dispatch(uidata.beginSaveUIData(keys));
      assert.lengthOf(_.keys(state), 2);
      assert.equal(state[key1].status, uidata.UIDataStatus.SAVING);
      assert.equal(state[key2].status, uidata.UIDataStatus.SAVING);
      dispatch(uidata.setUIDataKey(key1, "value1"));
      dispatch(uidata.setUIDataKey(key2, "value2"));
      assert.equal(state[key1].status, uidata.UIDataStatus.VALID);
      assert.equal(state[key2].status, uidata.UIDataStatus.VALID);
    });

    it("should correctly dispatch beginLoadUIData", function () {
      const key1 = "key1";
      const key2 = "key2";
      const keys = [key1, key2];
      dispatch(uidata.beginLoadUIData(keys));
      assert.lengthOf(_.keys(state), 2);
      assert.equal(state[key1].status, uidata.UIDataStatus.LOADING);
      assert.equal(state[key2].status, uidata.UIDataStatus.LOADING);
      dispatch(uidata.setUIDataKey(key1, "value1"));
      dispatch(uidata.setUIDataKey(key2, "value2"));
      assert.equal(state[key1].status, uidata.UIDataStatus.VALID);
      assert.equal(state[key2].status, uidata.UIDataStatus.VALID);
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
      return uidata.saveUIData.apply(this, values)(dispatch, () => {
        return { uiData: state };
      });
    };

    const loadUIData = function (...keys: string[]): Promise<void> {
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
          assert.equal(state[uiKey1].status, uidata.UIDataStatus.SAVING);
          assert.equal(state[uiKey2].status, uidata.UIDataStatus.SAVING);

          const kvs = protos.cockroach.server.serverpb.SetUIDataRequest.decode(
            new Uint8Array(requestObj.body as ArrayBuffer),
          ).key_values;

          assert.lengthOf(_.keys(kvs), 2);

          const deserialize = function (buffer: Uint8Array): Object {
            return JSON.parse(
              protobuf.util.utf8.read(buffer, 0, buffer.byteLength),
            );
          };

          assert.deepEqual(deserialize(kvs[uiKey1]), uiObj1);
          assert.deepEqual(deserialize(kvs[uiKey2]), uiObj2);

          const encodedResponse = protos.cockroach.server.serverpb.SetUIDataResponse.encode(
            {},
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
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
        assert.lengthOf(fetchMock.calls(`${api.API_PREFIX}/uidata`), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.equal(state[uiKey1].data, uiObj1);
        assert.equal(state[uiKey2].data, uiObj2);
        assert.isNull(state[uiKey1].error);
        assert.isNull(state[uiKey2].error);
        assert.equal(state[uiKey1].status, uidata.UIDataStatus.VALID);
        assert.equal(state[uiKey2].status, uidata.UIDataStatus.VALID);
      });
    });

    it("correctly reacts to error during save", function (done) {
      this.timeout(2000);
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
        assert.lengthOf(fetchMock.calls(`${api.API_PREFIX}/uidata`), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.equal(state[uiKey1].status, uidata.UIDataStatus.SAVING);
        assert.equal(state[uiKey2].status, uidata.UIDataStatus.SAVING);
        assert.isUndefined(state[uiKey1].data);
        assert.isUndefined(state[uiKey2].data);
        assert.notProperty(state[uiKey1], "data");
        assert.notProperty(state[uiKey2], "data");
        assert.isUndefined(state[uiKey1].error);
        assert.isUndefined(state[uiKey2].error);
        setTimeout(() => {
          assert.equal(state[uiKey1].status, uidata.UIDataStatus.SAVE_ERROR);
          assert.equal(state[uiKey2].status, uidata.UIDataStatus.SAVE_ERROR);
          assert.instanceOf(state[uiKey1].error, Error);
          assert.instanceOf(state[uiKey2].error, Error);
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
          assert.equal(state[uiKey1].status, uidata.UIDataStatus.LOADING);
          assert.equal(state[uiKey2].status, uidata.UIDataStatus.LOADING);

          const response: protos.cockroach.server.serverpb.IGetUIDataResponse = {
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

          const encodedResponse = protos.cockroach.server.serverpb.GetUIDataResponse.encode(
            response,
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      const p = loadUIData(uiKey1, uiKey2);
      const p2 = loadUIData(uiKey1, uiKey2); // Second load should be ignored.

      return Promise.all([p, p2]).then(() => {
        assert.lengthOf(fetchMock.calls(expectedURL), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.deepEqual(state[uiKey1].data, uiObj1);
        assert.deepEqual(state[uiKey2].data, uiObj2);
        assert.isNull(state[uiKey1].error);
        assert.isNull(state[uiKey2].error);
        assert.equal(state[uiKey1].status, uidata.UIDataStatus.VALID);
        assert.equal(state[uiKey2].status, uidata.UIDataStatus.VALID);
      });
    });

    it("correctly reacts to error during load", function (done) {
      this.timeout(2000);

      const uidataPrefixMatcher = `begin:${api.API_PREFIX}/uidata`;

      fetchMock.mock({
        matcher: uidataPrefixMatcher,
        response: () => {
          return { throws: new Error() };
        },
      });

      const p = loadUIData(uiKey1, uiKey2);

      p.then(() => {
        assert.lengthOf(fetchMock.calls(uidataPrefixMatcher), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.equal(state[uiKey1].status, uidata.UIDataStatus.LOADING);
        assert.equal(state[uiKey2].status, uidata.UIDataStatus.LOADING);
        assert.isUndefined(state[uiKey1].data);
        assert.isUndefined(state[uiKey2].data);
        assert.notProperty(state[uiKey1], "data");
        assert.notProperty(state[uiKey2], "data");
        assert.isUndefined(state[uiKey1].error);
        assert.isUndefined(state[uiKey2].error);
        setTimeout(() => {
          assert.equal(state[uiKey1].status, uidata.UIDataStatus.LOAD_ERROR);
          assert.equal(state[uiKey2].status, uidata.UIDataStatus.LOAD_ERROR);
          assert.instanceOf(state[uiKey1].error, Error);
          assert.instanceOf(state[uiKey2].error, Error);
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
          assert.equal(state[missingKey].status, uidata.UIDataStatus.LOADING);

          const encodedResponse = protos.cockroach.server.serverpb.GetUIDataResponse.encode(
            {},
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      const p = loadUIData(missingKey);

      return p.then(() => {
        assert.lengthOf(fetchMock.calls(expectedURL), 1);
        assert.lengthOf(_.keys(state), 1);
        assert.equal(state[missingKey].data, undefined);
        assert.property(state[missingKey], "data");
        assert.equal(state[missingKey].status, uidata.UIDataStatus.VALID);
        assert.isNull(state[missingKey].error);
      });
    });
  });
});
