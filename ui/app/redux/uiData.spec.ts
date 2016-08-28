import { assert } from "chai";
import _ = require("lodash");
import ByteBuffer = require("bytebuffer");
import * as fetchMock from "../util/fetch-mock";

import * as protos from "../js/protos";
import reducer, * as uidata from "./uiData";
import { Action } from "../interfaces/action";

describe("UIData reducer", function() {
  describe("actions", function() {
    it("setUIDataKey() creates the correct action type.", function() {
      assert.equal(uidata.setUIDataKey("string", null).type, uidata.SET);
    });

    it("errorUIData() creates the correct action type.", function() {
      assert.equal(uidata.errorUIData(null).type, uidata.ERROR);
    });

    it("fetchUIData() creates the correct action type.", function() {
      assert.equal(uidata.fetchUIData().type, uidata.FETCH);
    });

    it("fetchCompleteUIData() creates the correct action type.", function() {
      assert.equal(uidata.fetchCompleteUIData().type, uidata.FETCH_COMPLETE);
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
      let expected = {
        inFlight: 0,
        data: {},
      };
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

      assert.isDefined(state.data);
      assert.lengthOf(_.keys(state.data), 3);
      assert.equal(state.data[objKey], obj);
      assert.equal(state.data[boolKey], bool);
      assert.equal(state.data[numKey], num);

      // validate overwrite.
      let obj2 = { value: 2 };
      dispatch(uidata.setUIDataKey(objKey, obj2));
      assert.lengthOf(_.keys(state.data), 3);
      assert.equal(state.data[objKey], obj2);
    });

    it("should correctly dispatch errorUIData.", function() {
      let err = new Error("an error.");
      dispatch(uidata.errorUIData(err));
      assert.equal(state.error, err);

      dispatch(uidata.setUIDataKey("num", 4));
      assert.isNull(state.error);
    });

    it("should correctly dispatch fetchMetrics and fetchMetricsComplete", function() {
      dispatch(uidata.fetchUIData());
      assert.equal(state.inFlight, 1);
      dispatch(uidata.fetchUIData());
      assert.equal(state.inFlight, 2);
      dispatch(uidata.fetchCompleteUIData());
      assert.equal(state.inFlight, 1);
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
      return uidata.saveUIData.apply(this, values)(dispatch);
    };

    let loadUIData = function(...keys: string[]): Promise<void> {
      return uidata.loadUIData.apply(this, keys)(dispatch);
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
          assert.equal(state.inFlight, 1);

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

      return p.then(() => {
        assert.lengthOf(fetchMock.calls("/_admin/v1/uidata"), 1);
        assert.lengthOf(_.keys(state.data), 2);
        assert.equal(state.data[uiKey1], uiObj1);
        assert.equal(state.data[uiKey2], uiObj2);
        assert.isNull(state.error);
        assert.equal(state.inFlight, 0);
      });
    });

    it("correctly reacts to error during save", function() {
      fetchMock.mock({
        matcher: "/_admin/v1/uidata",
        method: "POST",
        response: () => {
          return { throws: new Error() };
        },
      });

      let p = saveUIData(
        {key: uiKey1, value: uiObj1},
        {key: uiKey2, value: uiObj2}
      );

      return p.then(() => {
        assert.lengthOf(fetchMock.calls("/_admin/v1/uidata"), 1);
        assert.lengthOf(_.keys(state.data), 0);
        assert.isNotNull(state.error);
        assert.equal(state.inFlight, 0);
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
          assert.equal(state.inFlight, 1);

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

      return p.then(() => {
        assert.lengthOf(fetchMock.calls(expectedURL), 1);
        assert.lengthOf(_.keys(state.data), 2);
        assert.deepEqual(state.data[uiKey1], uiObj1);
        assert.deepEqual(state.data[uiKey2], uiObj2);
        assert.isNull(state.error);
        assert.equal(state.inFlight, 0);
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
        assert.lengthOf(_.keys(state.data), 0);
        assert.isNotNull(state.error);
        assert.equal(state.inFlight, 0);
      });
    });

    it("handles missing keys", function () {
      let missingKey = "missingKey";

      let expectedURL = `/_admin/v1/uidata?keys=${missingKey}`;

      fetchMock.mock({
        matcher: expectedURL,
        response: () => {
          assert.equal(state.inFlight, 1);

          let response = new protos.cockroach.server.serverpb.GetUIDataResponse();

          return {
            body: response.toArrayBuffer(),
          };
        },
      });

      let p = loadUIData(missingKey);

      return p.then(() => {
        assert.lengthOf(fetchMock.calls(expectedURL), 1);
        assert.lengthOf(_.keys(state.data), 1);
        assert.deepEqual(state.data[missingKey], undefined);
        assert.isNull(state.error);
        assert.equal(state.inFlight, 0);
      });
    });
  });
});
