import { assert } from "chai";
import _ = require("lodash");
import * as fetchMock from "../util/fetch-mock";
import Long = require("long");

import * as protos from "../js/protos";
import * as api from "./api";

describe("rest api", function() {
  afterEach(function () {
    api.setFetchTimeout(10000);
  });

  describe("propsToQueryString", function () {
    interface PropBag {
      [k: string]: string;
    }

    // helper decoding function used to doublecheck querystring generation
    function decodeQueryString(qs: string): PropBag {
      return _.reduce<string, PropBag>(
        qs.split("&"),
        (memo: PropBag, v: string) => {
          let [key, value] = v.split("=");
          memo[decodeURIComponent(key)] = decodeURIComponent(value);
          return memo;
        },
        {}
      );
    }

    it("creates an appropriate querystring", function () {
      let testValues: any = {
        a: "testa",
        b: "testb",
      };

      let querystring = api.propsToQueryString(testValues);

      assert((/a=testa/).test(querystring));
      assert((/b=testb/).test(querystring));
      assert.lengthOf(querystring.match(/=/g), 2);
      assert.lengthOf(querystring.match(/&/g), 1);
      assert.deepEqual(testValues, decodeQueryString(querystring));
    });

    it("handles falsy values correctly", function () {
      let testValues: any = {
        // null and undefined should be ignored
        undefined: undefined,
        null: null,
        // other values should be added
        false: false,
        "": "",
        0: 0,
      };

      let querystring = api.propsToQueryString(testValues);

      assert((/false=false/).test(querystring));
      assert((/0=0/).test(querystring));
      assert((/([^A-Za-z]|^)=([^A-Za-z]|$)/).test(querystring));
      assert.lengthOf(querystring.match(/=/g), 3);
      assert.lengthOf(querystring.match(/&/g), 2);
      assert.notOk((/undefined/).test(querystring));
      assert.notOk((/null/).test(querystring));
      assert.deepEqual({ false: "false", "": "", 0: "0" }, decodeQueryString(querystring));
    });

    it("handles special characters", function () {
      let key = "!@#$%^&*()=+-_\\|\"`'?/<>";
      let value = key.split("").reverse().join(""); // key reversed
      let testValues: any = {
        [key] : value,
      };

      let querystring = api.propsToQueryString(testValues);

      assert(querystring.match(/%/g).length > (key + value).match(/%/g).length);
      assert.deepEqual(testValues, decodeQueryString(querystring));
    });

    it("handles non-string values", function () {
      let testValues: any = {
        boolean: true,
        number: 1,
        emptyObject: {},
        emptyArray: [],
        objectWithProps: { a: 1, b: 2 },
        arrayWithElts: [1, 2, 3],
        long: new Long(1),
      };

      let querystring = api.propsToQueryString(testValues);
      assert.deepEqual(_.mapValues(testValues, _.toString), decodeQueryString(querystring));
    });
  });

  describe("databases request", function () {
    afterEach(fetchMock.restore);

    it("correctly requests info about all databases", function () {
      this.timeout(1000);
      // Mock out the fetch query to /databases
      fetchMock.mock({
        matcher: api.API_PREFIX + "/databases",
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);

          return {
            body: new protos.cockroach.server.serverpb.DatabasesResponse({
              databases: ["system", "test"],
            }).toArrayBuffer(),
          };
        },
      });

      return api.getDatabaseList().then((result) => {
        assert.lengthOf(fetchMock.calls(api.API_PREFIX + "/databases"), 1);
        assert.lengthOf(result.databases, 2);
      });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);
      // Mock out the fetch query to /databases, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: api.API_PREFIX + "/databases",
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api.getDatabaseList().then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.isError(e));
        done();
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      api.setFetchTimeout(0);
      // Mock out the fetch query to /databases, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: api.API_PREFIX + "/databases",
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => { });
        },
      });

      api.getDatabaseList().then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.startsWith(e.message, "Promise timed out"), "Error is a timeout error.");
        done();
      });
    });

  });

  describe("database details request", function () {
    let dbName = "test";

    afterEach(fetchMock.restore);

    it("correctly requests info about a specific database", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}`,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return {
            body: new protos.cockroach.server.serverpb.DatabaseDetailsResponse({
              table_names: ["table1", "table2"],
              grants: [
                { user: "root", privileges: ["ALL"] },
                { user: "other", privileges: [] },
              ],
            }).toArrayBuffer(),
          };
        },
      });

      return api.getDatabaseDetails({ database: dbName }).then((result) => {
        assert.lengthOf(fetchMock.calls(`${api.API_PREFIX}/databases/${dbName}`), 1);
        assert.lengthOf(result.table_names, 2);
        assert.lengthOf(result.grants, 2);
      });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}`,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api.getDatabaseDetails({ database: dbName }).then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.isError(e));
        done();
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      api.setFetchTimeout(0);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}`,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => { });
        },
      });

      api.getDatabaseDetails({ database: dbName }).then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.startsWith(e.message, "Promise timed out"), "Error is a timeout error.");
        done();
      });
    });

  });

  describe("table details request", function () {
    let dbName = "testDB";
    let tableName = "testTable";

    afterEach(fetchMock.restore);

    it("correctly requests info about a specific table", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return {
            body: new protos.cockroach.server.serverpb.TableDetailsResponse().toArrayBuffer(),
          };
        },
      });

      return api.getTableDetails({ database: dbName, table: tableName }).then((result) => {
        assert.lengthOf(fetchMock.calls(`${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`), 1);
        assert.lengthOf(result.columns, 0);
        assert.lengthOf(result.indexes, 0);
        assert.lengthOf(result.grants, 0);
      });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api.getTableDetails({ database: dbName, table: tableName }).then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.isError(e));
        done();
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      api.setFetchTimeout(0);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => { });
        },
      });

      api.getTableDetails({ database: dbName, table: tableName }).then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.startsWith(e.message, "Promise timed out"), "Error is a timeout error.");
        done();
      });
    });

  });

  describe("events request", function() {
    let eventsUrl = `^${api.API_PREFIX}/events?`;

    afterEach(fetchMock.restore);

    it("correctly requests events", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock({
        matcher: api.API_PREFIX + "/events?",
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return {
            body: new protos.cockroach.server.serverpb.EventsResponse({
              events: [
                { event_type: "test" },
              ],
            }).toArrayBuffer(),
          };
        },
      });

      return api.getEvents().then((result) => {
        assert.lengthOf(fetchMock.calls(api.API_PREFIX + "/events?"), 1);
        assert.lengthOf(result.events, 1);
      });
    });

    it("correctly requests filtered events", function () {
      this.timeout(1000);

      let req = new protos.cockroach.server.serverpb.EventsRequest({
        target_id: new Long(1),
        type: "test type",
      });

      // Mock out the fetch query
      fetchMock.mock({
        matcher: eventsUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          let params = url.split("?")[1].split("&");
          assert.lengthOf(params, 2);
          _.each(params, (param) => {
            let [k, v] = param.split("=");
            assert.equal(String((req as any)[decodeURIComponent(k)]), decodeURIComponent(v));
          });
          assert.isUndefined(requestObj.body);
          return {
            body: new protos.cockroach.server.serverpb.EventsResponse({
              events: [
                { event_type: "test" },
              ],
            }).toArrayBuffer(),
          };
        },
      });

      return api.getEvents(req).then((result) => {
        assert.lengthOf(fetchMock.calls(eventsUrl), 1);
        assert.lengthOf(result.events, 1);
      });
    });

    it("ignores unknown parameters", function () {
      this.timeout(1000);

      let req = {
        fake: 1,
        blah: 2,
        type: "here",
      };

      // Mock out the fetch query
      fetchMock.mock({
        matcher: eventsUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          let params = url.split("?")[1].split("&");
          assert.lengthOf(params, 1);
          assert(params[0].split("=")[0] === "type");
          assert(params[0].split("=")[1] === "here");
          assert.isUndefined(requestObj.body);
          return {
            body: new protos.cockroach.server.serverpb.EventsResponse().toArrayBuffer(),
          };
        },
      });

      return api.getEvents(req).then((result) => {
        assert.lengthOf(fetchMock.calls(eventsUrl), 1);
        assert.lengthOf(result.events, 0);
      });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);

      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: eventsUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api.getEvents({}).then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.isError(e));
        done();
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      api.setFetchTimeout(0);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: eventsUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => { });
        },
      });

      api.getEvents().then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.startsWith(e.message, "Promise timed out"), "Error is a timeout error.");
        done();
      });
    });
  });

  describe("health request", function() {
    let healthUrl = `${api.API_PREFIX}/health`;

    afterEach(fetchMock.restore);

    it("correctly requests health", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock({
        matcher: healthUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return {
            body: new protos.cockroach.server.serverpb.HealthResponse().toArrayBuffer(),
          };
        },
      });

      return api.getHealth().then((result) => {
        assert.lengthOf(fetchMock.calls(healthUrl), 1);
        assert.deepEqual(result, new protos.cockroach.server.serverpb.HealthResponse());
      });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);

      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: healthUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api.getHealth().then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.isError(e));
        done();
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      api.setFetchTimeout(0);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: healthUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => { });
        },
      });

      api.getHealth().then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.startsWith(e.message, "Promise timed out"), "Error is a timeout error.");
        done();
      });
    });
  });

  describe("cluster request", function() {
    let clusterUrl = `${api.API_PREFIX}/cluster`;
    let clusterID = "12345abcde";

    afterEach(fetchMock.restore);

    it("correctly requests cluster info", function () {
      this.timeout(1000);
      fetchMock.mock({
        matcher: clusterUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return {
            body: new protos.cockroach.server.serverpb.ClusterResponse({ cluster_id: clusterID }).toArrayBuffer(),
          };
        },
      });

      return api.getCluster().then((result) => {
        assert.lengthOf(fetchMock.calls(clusterUrl), 1);
        assert.deepEqual(result.cluster_id, clusterID);
      });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);

      // Mock out the fetch query, but return an error
      fetchMock.mock({
        matcher: clusterUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api.getCluster().then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.isError(e));
        done();
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      api.setFetchTimeout(0);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: clusterUrl,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => { });
        },
      });

      api.getCluster().then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.startsWith(e.message, "Promise timed out"), "Error is a timeout error.");
        done();
      });
    });
  });
});
