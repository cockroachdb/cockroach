import { assert } from "chai";
import _ = require("lodash");
import ByteBuffer = require("bytebuffer");
import * as fetchMock from "fetch-mock";

import * as protos from "../js/protos";
import reducer, * as uidata from "./uiData";
import { Action } from "../interfaces/action";

describe("UIData reducer", function() {
  describe("actions", function() {
    it("setUIData() creates the correct action type.", function() {
      assert.equal(uidata.setUIData("string", null).type, uidata.SET);
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

    it("should correctly dispatch setUIData.", function() {
      let objKey = "obj";
      let boolKey = "bool";
      let numKey = "num";
      let obj = { value: 1 };
      let bool = true;
      let num = 240;

      // Validate setting a variety of object types.
      dispatch(uidata.setUIData(objKey, obj));
      dispatch(uidata.setUIData(boolKey, bool));
      dispatch(uidata.setUIData(numKey, num));

      assert.isDefined(state.data);
      assert.lengthOf(_.keys(state.data), 3);
      assert.equal(state.data[objKey], obj);
      assert.equal(state.data[boolKey], bool);
      assert.equal(state.data[numKey], num);

      // validate overwrite.
      let obj2 = { value: 2 };
      dispatch(uidata.setUIData(objKey, obj2));
      assert.lengthOf(_.keys(state.data), 3);
      assert.equal(state.data[objKey], obj2);
    });

    it("should correctly dispatch errorUIData.", function() {
      let err = new Error("an error.");
      dispatch(uidata.errorUIData(err));
      assert.equal(state.error, err);

      dispatch(uidata.setUIData("num", 4));
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

    afterEach(function () {
      fetchMock.restore();
    });

    it("correctly saves UIData", function() {

      fetchMock.mock("/_admin/v1/uidata", "post", (url: string, requestObj: RequestInit) => {
        assert.equal(state.inFlight, 1);
        let requestJSON = JSON.parse(requestObj.body as string);
        let request = new protos.cockroach.server.SetUIDataRequest(requestJSON);
        let kvs = request.getKeyValues();

        assert.equal(kvs.size, 2);

        let deserialize = function(buff: ByteBuffer): any {
          return JSON.parse(buff.readString(buff.limit));
        };

        assert.deepEqual(deserialize(kvs.get(uiKey1)), uiObj1);
        assert.deepEqual(deserialize(kvs.get(uiKey2)), uiObj2);

        return {
          body: new protos.cockroach.server.SetUIDataResponse(),
        };
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
      fetchMock.mock("/_admin/v1/uidata", "post", () => {
        return { status: 500 };
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
      fetchMock.mock(expectedURL, "get", function() {
        // FetchMock URL must match the above string exactly, requesting both
        // keys.
        assert.equal(state.inFlight, 1);

        let response = new protos.cockroach.server.GetUIDataResponse();
        let setValue = function(key: string, obj: any) {
          let value = new protos.cockroach.server.GetUIDataResponse.Value();
          value.setValue(ByteBuffer.fromUTF8(JSON.stringify(obj)));
          response.key_values.set(key, value);
        };
        setValue(uiKey1, uiObj1);
        setValue(uiKey2, uiObj2);

        return {
          body: response.encodeJSON(),
        };
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
      fetchMock.mock("^/_admin/v1/uidata" /* "^" allows prefix match */, "get", () => {
        return { status: 500 };
      });

      let p = loadUIData(uiKey1, uiKey2);

      return p.then(() => {
        assert.lengthOf(fetchMock.calls("^/_admin/v1/uidata"), 1);
        assert.lengthOf(_.keys(state.data), 0);
        assert.isNotNull(state.error);
        assert.equal(state.inFlight, 0);
      });
    });
  });
});
