import { assert } from "chai";
import _ from "lodash";
import ByteBuffer from "bytebuffer";
import fetchMock from "../util/fetch-mock";

import * as protos from "../js/protos";
import reducer from "./uiData";
import * as uidata from "./uiData";

import { Action } from "../interfaces/action";

describe("UIData reducer", function() {
  describe("actions", function() {
    it("setUIDataKey() creates the correct action type.", function() {
      assert.equal(uidata.setUIDataKey("string", null).type, uidata.SET);
    });

    it("beginSaveUIData() creates the correct action type.", function() {
      assert.equal(uidata.beginSaveUIData([]).type, uidata.SAVE);
    });

    it("completeSaveUIData() creates the correct action type.", function() {
      assert.equal(uidata.completeSaveUIData([]).type, uidata.SAVE_COMPLETE);
    });

    it("saveErrorUIData() creates the correct action type.", function() {
      assert.equal(uidata.saveErrorUIData(null, null).type, uidata.SAVE_ERROR);
    });

    it("beginLoadUIData() creates the correct action type.", function() {
      assert.equal(uidata.beginLoadUIData([]).type, uidata.LOAD);
    });

    it("completeSaveUIData() creates the correct action type.", function() {
      assert.equal(uidata.completeLoadUIData([]).type, uidata.LOAD_COMPLETE);
    });

    it("loadErrorUIData() creates the correct action type.", function() {
      assert.equal(uidata.loadErrorUIData(null, null).type, uidata.LOAD_ERROR);
    });
  });

  describe("reducer", function() {
    let state: uidata.UIDataSet;

    beforeEach(function () {
      state = reducer(undefined, { type: "unknown" });
    });

    let dispatch = (action: Action) => {
      state = reducer(state, action);
    };

    it("should have the correct default value.", function() {
      let expected = {};
      assert.deepEqual(state, expected);
    });

    it("should correctly dispatch setUIDataKey.", function() {
      let objKey = "obj";
      let boolKey = "bool";
      let numKey = "num";
      let obj = { value: 1 };
      let bool = true;
      let num = 240;

      // Validate setting a variety of object types.
      dispatch(uidata.setUIDataKey(objKey, obj));
      dispatch(uidata.setUIDataKey(boolKey, bool));
      dispatch(uidata.setUIDataKey(numKey, num));

      assert.lengthOf(_.keys(state), 3);
      assert.equal(state[objKey].data, obj);
      assert.equal(state[boolKey].data, bool);
      assert.equal(state[numKey].data, num);

      // validate overwrite.
      let obj2 = { value: 2 };
      dispatch(uidata.setUIDataKey(objKey, obj2));
      assert.lengthOf(_.keys(state), 3);
      assert.equal(state[objKey].data, obj2);
    });

    it("should correctly dispatch loadErrorUIData.", function () {
      let key1 = "key1";
      let key2 = "key2";
      let err = new Error("an error.");
      dispatch(uidata.loadErrorUIData(key1, err));
      assert.equal(state[key1].lastLoadError, err);

      dispatch(uidata.setUIDataKey(key2, 4));
      assert.isNull(state[key2].lastLoadError);
    });

    it("should correctly dispatch saveErrorUIData.", function () {
      let key1 = "key1";
      let key2 = "key2";
      let err = new Error("an error.");
      dispatch(uidata.saveErrorUIData(key1, err));
      assert.equal(state[key1].lastSaveError, err);

      dispatch(uidata.setUIDataKey(key2, 4));
      assert.isNull(state[key2].lastSaveError);
    });

    it("should correctly dispatch beginSaveUIData and completeSaveUIData", function () {
      let key1 = "key1";
      let key2 = "key2";
      let keys = [key1, key2];
      dispatch(uidata.beginSaveUIData(keys));
      assert.lengthOf(_.keys(state), 2);
      assert(state[key1].saving);
      assert(state[key2].saving);
      dispatch(uidata.completeSaveUIData(keys));
      assert.isFalse(state[key1].saving);
      assert.isFalse(state[key2].saving);
    });

    it("should correctly dispatch beginLoadUIData and completeLoadUIData", function () {
      let key1 = "key1";
      let key2 = "key2";
      let keys = [key1, key2];
      dispatch(uidata.beginLoadUIData(keys));
      assert.lengthOf(_.keys(state), 2);
      assert(state[key1].loading);
      assert(state[key2].loading);
      dispatch(uidata.completeLoadUIData(keys));
      assert.isFalse(state[key1].loading);
      assert.isFalse(state[key2].loading);
    });
  });

  describe("asynchronous actions", function() {
    let state: uidata.UIDataSet;

    let dispatch = (action: Action) => {
      state = reducer(state, action);
    };

    let uiKey1 = "a_key";
    let uiObj1 = {
      setting1: "value",
      setting2: true,
    };

    let uiKey2 = "another_key";
    let uiObj2 = 1234;

    let saveUIData = function(...values: uidata.KeyValue[]): Promise<void> {
      return uidata.saveUIData.apply(this, values)(dispatch, () => { return { uiData: state }; });
    };

    let loadUIData = function(...keys: string[]): Promise<void> {
      return uidata.loadUIData.apply(this, keys)(dispatch, () => { return { uiData: state }; });
    };

    beforeEach(function () {
      state = reducer(undefined, { type: "unknown" });
    });

    afterEach(fetchMock.restore);

    it("correctly saves UIData", function() {
      fetchMock.mock({
        matcher: "/_admin/v1/uidata",
        method: "POST",
        response: (url: string, requestObj: RequestInit) => {
          assert(state[uiKey1].saving);
          assert(state[uiKey2].saving);

          let kvs = protos.cockroach.server.serverpb.SetUIDataRequest.decode(requestObj.body as ArrayBuffer).getKeyValues();

          assert.equal(kvs.size, 2);

          let deserialize = function(buff: ByteBuffer): Object {
            return JSON.parse(buff.readString(buff.limit - buff.offset));
          };

          assert.deepEqual(deserialize(kvs.get(uiKey1)), uiObj1);
          assert.deepEqual(deserialize(kvs.get(uiKey2)), uiObj2);

          return {
            body: new protos.cockroach.server.serverpb.SetUIDataResponse().toArrayBuffer(),
          };
        },
      });

      let p = saveUIData(
        {key: uiKey1, value: uiObj1},
        {key: uiKey2, value: uiObj2}
      );

      // Second save should be ignored.
      let p2 = saveUIData(
        {key: uiKey1, value: uiObj1},
        {key: uiKey2, value: uiObj2}
      );

      return Promise.all([p, p2]).then(() => {
        assert.lengthOf(fetchMock.calls("/_admin/v1/uidata"), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.equal(state[uiKey1].data, uiObj1);
        assert.equal(state[uiKey2].data, uiObj2);
        assert.isNull(state[uiKey1].lastSaveError);
        assert.isNull(state[uiKey1].lastLoadError);
        assert.isNull(state[uiKey2].lastSaveError);
        assert.isNull(state[uiKey2].lastLoadError);
        assert.isFalse(state[uiKey1].saving);
        assert.isFalse(state[uiKey1].loading);
        assert.isFalse(state[uiKey2].saving);
        assert.isFalse(state[uiKey2].loading);
      });
    });

    it("correctly reacts to error during save", function() {
      fetchMock.mock({
        matcher: "/_admin/v1/uidata",
        method: "POST",
        response: () => {
          return { throws: new Error(), status: 500};
        },
      });

      let p = saveUIData(
        {key: uiKey1, value: uiObj1},
        {key: uiKey2, value: uiObj2}
      );

      return p.then(() => {
        assert.lengthOf(fetchMock.calls("/_admin/v1/uidata"), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.isFalse(state[uiKey1].saving);
        assert.isFalse(state[uiKey2].saving);
        assert.isUndefined(state[uiKey1].data);
        assert.isUndefined(state[uiKey2].data);
        assert.notProperty(state[uiKey1], "data");
        assert.notProperty(state[uiKey2], "data");
        assert.instanceOf(state[uiKey1].lastSaveError, Error);
        assert.isUndefined(state[uiKey1].lastLoadError);
        assert.instanceOf(state[uiKey2].lastSaveError, Error);
        assert.isUndefined(state[uiKey2].lastLoadError);
      });
    });

    it("correctly loads UIData", function() {
      let expectedURL = `/_admin/v1/uidata?keys=${uiKey1}&keys=${uiKey2}`;

      fetchMock.mock({
        matcher: expectedURL,
        method: "GET",
        response: () => {
          // FetchMock URL must match the above string exactly, requesting both
          // keys.
          assert(state[uiKey1].loading);
          assert(state[uiKey2].loading);

          let response = new protos.cockroach.server.serverpb.GetUIDataResponse();
          let setValue = function(key: string, obj: Object) {
            let value = new protos.cockroach.server.serverpb.GetUIDataResponse.Value();
            value.setValue(ByteBuffer.fromUTF8(JSON.stringify(obj)));
            response.key_values.set(key, value);
          };
          setValue(uiKey1, uiObj1);
          setValue(uiKey2, uiObj2);

          return {
            body: response.toArrayBuffer(),
          };
        },
      });

      let p = loadUIData(uiKey1, uiKey2);
      let p2 = loadUIData(uiKey1, uiKey2); // Second load should be ignored.

      return Promise.all([p, p2]).then(() => {
        assert.lengthOf(fetchMock.calls(expectedURL), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.deepEqual(state[uiKey1].data, uiObj1);
        assert.deepEqual(state[uiKey2].data, uiObj2);
        assert.isNull(state[uiKey1].lastSaveError);
        assert.isNull(state[uiKey1].lastLoadError);
        assert.isNull(state[uiKey2].lastSaveError);
        assert.isNull(state[uiKey2].lastLoadError);
        assert.isFalse(state[uiKey1].saving);
        assert.isFalse(state[uiKey1].loading);
        assert.isFalse(state[uiKey2].saving);
        assert.isFalse(state[uiKey2].loading);
      });
    });

    it("correctly reacts to error during load", function() {
      fetchMock.mock({
        matcher: "^/_admin/v1/uidata" /* "^" allows prefix match */,
        response: () => {
          return { throws: new Error() };
        },
      });

      let p = loadUIData(uiKey1, uiKey2);

      return p.then(() => {
        assert.lengthOf(fetchMock.calls("^/_admin/v1/uidata"), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.isFalse(state[uiKey1].loading);
        assert.isFalse(state[uiKey2].loading);
        assert.isUndefined(state[uiKey1].data);
        assert.isUndefined(state[uiKey2].data);
        assert.notProperty(state[uiKey1], "data");
        assert.notProperty(state[uiKey2], "data");
        assert.instanceOf(state[uiKey1].lastLoadError, Error);
        assert.isUndefined(state[uiKey1].lastSaveError);
        assert.instanceOf(state[uiKey2].lastLoadError, Error);
        assert.isUndefined(state[uiKey2].lastSaveError);
      });
    });

    it("handles missing keys", function () {
      let missingKey = "missingKey";

      let expectedURL = `/_admin/v1/uidata?keys=${missingKey}`;

      fetchMock.mock({
        matcher: expectedURL,
        response: () => {
          assert(state[missingKey].loading);

          let response = new protos.cockroach.server.serverpb.GetUIDataResponse();

          return {
            body: response.toArrayBuffer(),
          };
        },
      });

      let p = loadUIData(missingKey);

      return p.then(() => {
        assert.lengthOf(fetchMock.calls(expectedURL), 1);
        assert.lengthOf(_.keys(state), 1);
        assert.equal(state[missingKey].data, undefined);
        assert.property(state[missingKey], "data");
        assert.isNull(state[missingKey].lastLoadError);
        assert.isNull(state[missingKey].lastSaveError);
        assert.isFalse(state[missingKey].saving);
        assert.isFalse(state[missingKey].loading);
      });
    });
  });
});
