import { assert } from "chai";
import _ = require("lodash");
import * as fetchMock from "fetch-mock";

import {getDatabaseList, getDatabaseDetails, getTableDetails, API_PREFIX, setFetchTimeout} from "./api";

describe("rest api", function() {
  describe("databases request", function () {
    afterEach(function () {
      fetchMock.restore();
    });

    it("correctly requests info about all databases", function () {
      this.timeout(1000);
      // Mock out the fetch query to /databases
      fetchMock.mock(API_PREFIX + "/databases", "get", (url: string, requestObj: RequestInit) => {

        assert.isUndefined(requestObj.body);

        return { databases: ["system", "test"] };
      });

      return getDatabaseList().then((result) => {
        assert.lengthOf(fetchMock.calls(API_PREFIX + "/databases"), 1);
        assert.lengthOf(result.databases, 2);
      });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);
      // Mock out the fetch query to /databases, but return a promise that's never resolved to test the timeout
      fetchMock.mock(API_PREFIX + "/databases", "get", (url: string, requestObj: RequestInit) => {
        assert.isUndefined(requestObj.body);
        return { status: 500 };
      });

      getDatabaseList().then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.isError(e));
        done();
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      setFetchTimeout(0);
      // Mock out the fetch query to /databases, but return a promise that's never resolved to test the timeout
      fetchMock.mock(API_PREFIX + "/databases", "get", (url: string, requestObj: RequestInit) => {
        assert.isUndefined(requestObj.body);
        return new Promise<any>(() => { });
      });

      getDatabaseList().then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.startsWith(e.message, "Promise timed out"), "Error is a timeout error.");
        done();
      });
    });

  });

  describe("database details request", function () {

    let dbName = "test";

    afterEach(function () {
      fetchMock.restore();
    });

    it("correctly requests info about a specific database", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock(`${API_PREFIX}/databases/${dbName}`, "get", (url: string, requestObj: RequestInit) => {
        assert.isUndefined(requestObj.body);
        return { table_names: ["table1", "table2"], grants: [{ user: "root", privileges: ["ALL"] }, { user: "other", privileges: [] }] };
      });

      return getDatabaseDetails({ database: dbName }).then((result) => {
        assert.lengthOf(fetchMock.calls(`${API_PREFIX}/databases/${dbName}`), 1);
        assert.lengthOf(result.table_names, 2);
        assert.lengthOf(result.grants, 2);
      });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock(`${API_PREFIX}/databases/${dbName}`, "get", (url: string, requestObj: RequestInit) => {
        assert.isUndefined(requestObj.body);
        return { status: 500 };
      });

      getDatabaseDetails({ database: dbName }).then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.isError(e));
        done();
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      setFetchTimeout(0);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock(`${API_PREFIX}/databases/${dbName}`, "get", (url: string, requestObj: RequestInit) => {
        assert.isUndefined(requestObj.body);
        return new Promise<any>(() => { });
      });

      getDatabaseDetails({ database: dbName }).then((result) => {
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

    afterEach(function () {
      fetchMock.restore();
    });

    it("correctly requests info about a specific table", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock(`${API_PREFIX}/databases/${dbName}/tables/${tableName}`, "get", (url: string, requestObj: RequestInit) => {
        assert.isUndefined(requestObj.body);
        return { columns: [], grants: [], indexes: [] };
      });

      return getTableDetails({ database: dbName, table: tableName }).then((result) => {
        assert.lengthOf(fetchMock.calls(`${API_PREFIX}/databases/${dbName}/tables/${tableName}`), 1);
        assert.lengthOf(result.columns, 0);
        assert.lengthOf(result.indexes, 0);
        assert.lengthOf(result.grants, 0);
      });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock(`${API_PREFIX}/databases/${dbName}/tables/${tableName}`, "get", (url: string, requestObj: RequestInit) => {
        assert.isUndefined(requestObj.body);
        return { status: 500 };
      });

      getTableDetails({ database: dbName, table: tableName }).then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.isError(e));
        done();
      });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      setFetchTimeout(0);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock(`${API_PREFIX}/databases/${dbName}/tables/${tableName}`, "get", (url: string, requestObj: RequestInit) => {
        assert.isUndefined(requestObj.body);
        return new Promise<any>(() => { });
      });

      getTableDetails({ database: dbName, table: tableName }).then((result) => {
        done(new Error("Request unexpectedly succeeded."));
      }).catch(function (e) {
        assert(_.startsWith(e.message, "Promise timed out"), "Error is a timeout error.");
        done();
      });
    });

  });
});
