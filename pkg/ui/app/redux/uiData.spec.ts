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

    it("saveErrorUIData() creates the correct action type.", function() {
      assert.equal(uidata.saveErrorUIData(null, null).type, uidata.SAVE_ERROR);
    });

    it("beginLoadUIData() creates the correct action type.", function() {
      assert.equal(uidata.beginLoadUIData([]).type, uidata.LOAD);
    });

    it("loadErrorUIData() creates the correct action type.", function() {
      assert.equal(uidata.loadErrorUIData(null, null).type, uidata.LOAD_ERROR);
    });
  });

  describe("helper functions", function () {
    let state: any;

    beforeEach(function () {
      state = { uiData: reducer(undefined, { type: "unknown" }) };
    });

    let dispatch = (action: Action) => {
      state = { uiData: reducer(state.uiData, action) };
    };

    it("isValid", function () {
      let key1 = "key1";
      let key2 = "key2";

      assert.isFalse(uidata.isValid(state, key1));
      assert.isFalse(uidata.isValid(state, key2));

      dispatch(uidata.setUIDataKey(key1, null));

      assert(uidata.isValid(state, key1));
      assert.isFalse(uidata.isValid(state, key2));

      dispatch(uidata.setUIDataKey(key2, null));

      assert(uidata.isValid(state, key1));
      assert(uidata.isValid(state, key2));
    });

    it("getData", function() {
      let key1 = "key1";
      let key2 = "key2";
      let value1 = "value1";
      let value2 = "value2";

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
      let key1 = "key1";
      let key2 = "key2";
      let saving = (k: string) => uidata.isSaving(state, k);
      let inFlight = (k: string) => uidata.isInFlight(state, k);

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
      let key1 = "key1";
      let key2 = "key2";
      let saveError = (k: string) => uidata.getSaveError(state, k);
      let loadError = (k: string) => uidata.getLoadError(state, k);

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

      assert.isUndefined(state[objKey]);
      assert.isUndefined(state[boolKey]);
      assert.isUndefined(state[numKey]);

      // Validate setting a variety of object types.
      dispatch(uidata.setUIDataKey(objKey, obj));
      dispatch(uidata.setUIDataKey(boolKey, bool));
      dispatch(uidata.setUIDataKey(numKey, num));

      assert.lengthOf(_.keys(state), 3);
      assert.equal(state[objKey].data, obj);
      assert.equal(state[objKey].state, uidata.UIDataState.VALID);
      assert.equal(state[boolKey].data, bool);
      assert.equal(state[boolKey].state, uidata.UIDataState.VALID);
      assert.equal(state[numKey].data, num);
      assert.equal(state[numKey].state, uidata.UIDataState.VALID);

      // validate overwrite.
      let obj2 = { value: 2 };
      dispatch(uidata.setUIDataKey(objKey, obj2));
      assert.lengthOf(_.keys(state), 3);
      assert.equal(state[objKey].data, obj2);
      assert.equal(state[objKey].state, uidata.UIDataState.VALID);
    });

    it("should correctly dispatch loadErrorUIData.", function () {
      let key1 = "key1";
      let err = new Error("an error.");
      assert.isUndefined(state[key1]);
      dispatch(uidata.loadErrorUIData(key1, err));
      assert.equal(state[key1].state, uidata.UIDataState.LOAD_ERROR);
      assert.equal(state[key1].error, err);

      dispatch(uidata.setUIDataKey(key1, 4));
      assert.equal(state[key1].state, uidata.UIDataState.VALID);
      assert.isNull(state[key1].error);
    });

    it("should correctly dispatch saveErrorUIData.", function () {
      let key1 = "key1";
      let err = new Error("an error.");
      assert.isUndefined(state[key1]);
      dispatch(uidata.saveErrorUIData(key1, err));
      assert.equal(state[key1].state, uidata.UIDataState.SAVE_ERROR);
      assert.equal(state[key1].error, err);

      dispatch(uidata.setUIDataKey(key1, 4));
      assert.equal(state[key1].state, uidata.UIDataState.VALID);
      assert.isNull(state[key1].error);
    });

    it("should correctly dispatch beginSaveUIData", function () {
      let key1 = "key1";
      let key2 = "key2";
      let keys = [key1, key2];
      dispatch(uidata.beginSaveUIData(keys));
      assert.lengthOf(_.keys(state), 2);
      assert.equal(state[key1].state, uidata.UIDataState.SAVING);
      assert.equal(state[key2].state, uidata.UIDataState.SAVING);
      dispatch(uidata.setUIDataKey(key1, "value1"));
      dispatch(uidata.setUIDataKey(key2, "value2"));
      assert.equal(state[key1].state, uidata.UIDataState.VALID);
      assert.equal(state[key2].state, uidata.UIDataState.VALID);
    });

    it("should correctly dispatch beginLoadUIData", function () {
      let key1 = "key1";
      let key2 = "key2";
      let keys = [key1, key2];
      dispatch(uidata.beginLoadUIData(keys));
      assert.lengthOf(_.keys(state), 2);
      assert.equal(state[key1].state, uidata.UIDataState.LOADING);
      assert.equal(state[key2].state, uidata.UIDataState.LOADING);
      dispatch(uidata.setUIDataKey(key1, "value1"));
      dispatch(uidata.setUIDataKey(key2, "value2"));
      assert.equal(state[key1].state, uidata.UIDataState.VALID);
      assert.equal(state[key2].state, uidata.UIDataState.VALID);
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
          assert.equal(state[uiKey1].state, uidata.UIDataState.SAVING);
          assert.equal(state[uiKey2].state, uidata.UIDataState.SAVING);

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
        assert.isNull(state[uiKey1].error);
        assert.isNull(state[uiKey2].error);
        assert.equal(state[uiKey1].state, uidata.UIDataState.VALID);
        assert.equal(state[uiKey2].state, uidata.UIDataState.VALID);
      });
    });

    it("correctly reacts to error during save", function (done) {
      this.timeout(2000);
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

      p.then(() => {
        assert.lengthOf(fetchMock.calls("/_admin/v1/uidata"), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.equal(state[uiKey1].state, uidata.UIDataState.SAVING);
        assert.equal(state[uiKey2].state, uidata.UIDataState.SAVING);
        assert.isUndefined(state[uiKey1].data);
        assert.isUndefined(state[uiKey2].data);
        assert.notProperty(state[uiKey1], "data");
        assert.notProperty(state[uiKey2], "data");
        assert.isUndefined(state[uiKey1].error);
        assert.isUndefined(state[uiKey2].error);
        setTimeout(
          () => {
            assert.equal(state[uiKey1].state, uidata.UIDataState.SAVE_ERROR);
            assert.equal(state[uiKey2].state, uidata.UIDataState.SAVE_ERROR);
            assert.instanceOf(state[uiKey1].error, Error);
            assert.instanceOf(state[uiKey2].error, Error);
            done();
          },
          1000
        );
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
          assert.equal(state[uiKey1].state, uidata.UIDataState.LOADING);
          assert.equal(state[uiKey2].state, uidata.UIDataState.LOADING);

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
        assert.isNull(state[uiKey1].error);
        assert.isNull(state[uiKey2].error);
        assert.equal(state[uiKey1].state, uidata.UIDataState.VALID);
        assert.equal(state[uiKey2].state, uidata.UIDataState.VALID);
      });
    });

    it("correctly reacts to error during load", function (done) {
      this.timeout(2000);
      fetchMock.mock({
        matcher: "^/_admin/v1/uidata" /* "^" allows prefix match */,
        response: () => {
          return { throws: new Error() };
        },
      });

      let p = loadUIData(uiKey1, uiKey2);

      p.then(() => {
        assert.lengthOf(fetchMock.calls("^/_admin/v1/uidata"), 1);
        assert.lengthOf(_.keys(state), 2);
        assert.equal(state[uiKey1].state, uidata.UIDataState.LOADING);
        assert.equal(state[uiKey2].state, uidata.UIDataState.LOADING);
        assert.isUndefined(state[uiKey1].data);
        assert.isUndefined(state[uiKey2].data);
        assert.notProperty(state[uiKey1], "data");
        assert.notProperty(state[uiKey2], "data");
        assert.isUndefined(state[uiKey1].error);
        assert.isUndefined(state[uiKey2].error);
        setTimeout(
          () => {
            assert.equal(state[uiKey1].state, uidata.UIDataState.LOAD_ERROR);
            assert.equal(state[uiKey2].state, uidata.UIDataState.LOAD_ERROR);
            assert.instanceOf(state[uiKey1].error, Error);
            assert.instanceOf(state[uiKey2].error, Error);
            done();
          },
          1000
        );
      });
    });

    it("handles missing keys", function () {
      let missingKey = "missingKey";

      let expectedURL = `/_admin/v1/uidata?keys=${missingKey}`;

      fetchMock.mock({
        matcher: expectedURL,
        response: () => {
          assert.equal(state[missingKey].state, uidata.UIDataState.LOADING);

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
        assert.equal(state[missingKey].state, uidata.UIDataState.VALID);
        assert.isNull(state[missingKey].error);
      });
    });
  });
});
