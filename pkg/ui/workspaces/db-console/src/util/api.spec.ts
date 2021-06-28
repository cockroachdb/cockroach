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
import moment from "moment";
import Long from "long";

import fetchMock from "./fetch-mock";

import * as protos from "src/js/protos";
import { cockroach } from "src/js/protos";
import * as api from "./api";
import { REMOTE_DEBUGGING_ERROR_TEXT } from "src/util/constants";
import Severity = cockroach.util.log.Severity;

describe("rest api", function () {
  describe("propsToQueryString", function () {
    interface PropBag {
      [k: string]: string;
    }

    // helper decoding function used to doublecheck querystring generation
    function decodeQueryString(qs: string): PropBag {
      return _.reduce<string, PropBag>(
        qs.split("&"),
        (memo: PropBag, v: string) => {
          const [key, value] = v.split("=");
          memo[decodeURIComponent(key)] = decodeURIComponent(value);
          return memo;
        },
        {},
      );
    }

    it("creates an appropriate querystring", function () {
      const testValues: { [k: string]: any } = {
        a: "testa",
        b: "testb",
      };

      const querystring = api.propsToQueryString(testValues);

      assert(/a=testa/.test(querystring));
      assert(/b=testb/.test(querystring));
      assert.lengthOf(querystring.match(/=/g), 2);
      assert.lengthOf(querystring.match(/&/g), 1);
      assert.deepEqual(testValues, decodeQueryString(querystring));
    });

    it("handles falsy values correctly", function () {
      const testValues: { [k: string]: any } = {
        // null and undefined should be ignored
        undefined: undefined,
        null: null,
        // other values should be added
        false: false,
        "": "",
        0: 0,
      };

      const querystring = api.propsToQueryString(testValues);

      assert(/false=false/.test(querystring));
      assert(/0=0/.test(querystring));
      assert(/([^A-Za-z]|^)=([^A-Za-z]|$)/.test(querystring));
      assert.lengthOf(querystring.match(/=/g), 3);
      assert.lengthOf(querystring.match(/&/g), 2);
      assert.notOk(/undefined/.test(querystring));
      assert.notOk(/null/.test(querystring));
      assert.deepEqual(
        { false: "false", "": "", 0: "0" },
        decodeQueryString(querystring),
      );
    });

    it("handles special characters", function () {
      const key = "!@#$%^&*()=+-_\\|\"`'?/<>";
      const value = key.split("").reverse().join(""); // key reversed
      const testValues: { [k: string]: any } = {
        [key]: value,
      };

      const querystring = api.propsToQueryString(testValues);

      assert(querystring.match(/%/g).length > (key + value).match(/%/g).length);
      assert.deepEqual(testValues, decodeQueryString(querystring));
    });

    it("handles non-string values", function () {
      const testValues: { [k: string]: any } = {
        boolean: true,
        number: 1,
        emptyObject: {},
        emptyArray: [],
        objectWithProps: { a: 1, b: 2 },
        arrayWithElts: [1, 2, 3],
        long: Long.fromNumber(1),
      };

      const querystring = api.propsToQueryString(testValues);
      assert.deepEqual(
        _.mapValues(testValues, _.toString),
        decodeQueryString(querystring),
      );
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
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          const encodedResponse = protos.cockroach.server.serverpb.DatabasesResponse.encode(
            {
              databases: ["system", "test"],
            },
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      return api
        .getDatabaseList(
          new protos.cockroach.server.serverpb.DatabasesRequest(),
        )
        .then((result) => {
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
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api
        .getDatabaseList(
          new protos.cockroach.server.serverpb.DatabasesRequest(),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(_.isError(e));
          done();
        });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      // Mock out the fetch query to /databases, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: api.API_PREFIX + "/databases",
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => {});
        },
      });

      api
        .getDatabaseList(
          new protos.cockroach.server.serverpb.DatabasesRequest(),
          moment.duration(0),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(
            _.startsWith(e.message, "Promise timed out"),
            "Error is a timeout error.",
          );
          done();
        });
    });
  });

  describe("database details request", function () {
    const dbName = "test";

    afterEach(fetchMock.restore);

    it("correctly requests info about a specific database", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          const encodedResponse = protos.cockroach.server.serverpb.DatabaseDetailsResponse.encode(
            {
              table_names: ["table1", "table2"],
              grants: [
                { user: "root", privileges: ["ALL"] },
                { user: "other", privileges: [] },
              ],
            },
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      return api
        .getDatabaseDetails(
          new protos.cockroach.server.serverpb.DatabaseDetailsRequest({
            database: dbName,
          }),
        )
        .then((result) => {
          assert.lengthOf(
            fetchMock.calls(`${api.API_PREFIX}/databases/${dbName}`),
            1,
          );
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
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api
        .getDatabaseDetails(
          new protos.cockroach.server.serverpb.DatabaseDetailsRequest({
            database: dbName,
          }),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(_.isError(e));
          done();
        });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => {});
        },
      });

      api
        .getDatabaseDetails(
          new protos.cockroach.server.serverpb.DatabaseDetailsRequest({
            database: dbName,
          }),
          moment.duration(0),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(
            _.startsWith(e.message, "Promise timed out"),
            "Error is a timeout error.",
          );
          done();
        });
    });
  });

  describe("table details request", function () {
    const dbName = "testDB";
    const tableName = "testTable";

    afterEach(fetchMock.restore);

    it("correctly requests info about a specific table", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          const encodedResponse = protos.cockroach.server.serverpb.TableDetailsResponse.encode(
            {},
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      return api
        .getTableDetails(
          new protos.cockroach.server.serverpb.TableDetailsRequest({
            database: dbName,
            table: tableName,
          }),
        )
        .then((result) => {
          assert.lengthOf(
            fetchMock.calls(
              `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
            ),
            1,
          );
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
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api
        .getTableDetails(
          new protos.cockroach.server.serverpb.TableDetailsRequest({
            database: dbName,
            table: tableName,
          }),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(_.isError(e));
          done();
        });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => {});
        },
      });

      api
        .getTableDetails(
          new protos.cockroach.server.serverpb.TableDetailsRequest({
            database: dbName,
            table: tableName,
          }),
          moment.duration(0),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(
            _.startsWith(e.message, "Promise timed out"),
            "Error is a timeout error.",
          );
          done();
        });
    });
  });

  describe("events request", function () {
    const eventsPrefixMatcher = `begin:${api.API_PREFIX}/events?`;

    afterEach(fetchMock.restore);

    it("correctly requests events", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock({
        matcher: eventsPrefixMatcher,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          const encodedResponse = protos.cockroach.server.serverpb.EventsResponse.encode(
            {
              events: [{ event_type: "test" }],
            },
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      return api
        .getEvents(new protos.cockroach.server.serverpb.EventsRequest())
        .then((result) => {
          assert.lengthOf(fetchMock.calls(eventsPrefixMatcher), 1);
          assert.lengthOf(result.events, 1);
        });
    });

    it("correctly requests filtered events", function () {
      this.timeout(1000);

      const req = new protos.cockroach.server.serverpb.EventsRequest({
        target_id: Long.fromNumber(1),
        type: "test type",
      });

      // Mock out the fetch query
      fetchMock.mock({
        matcher: eventsPrefixMatcher,
        method: "GET",
        response: (url: string, requestObj: RequestInit) => {
          const params = url.split("?")[1].split("&");
          assert.lengthOf(params, 3);
          _.each(params, (param) => {
            let [k, v] = param.split("=");
            k = decodeURIComponent(k);
            v = decodeURIComponent(v);
            switch (k) {
              case "target_id":
                assert.equal(req.target_id.toString(), v);
                break;

              case "type":
                assert.equal(req.type, v);
                break;

              case "unredacted_events":
                break;

              default:
                throw new Error(`Unknown property ${k}`);
            }
          });
          assert.isUndefined(requestObj.body);
          const encodedResponse = protos.cockroach.server.serverpb.EventsResponse.encode(
            {
              events: [{ event_type: "test" }],
            },
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      return api
        .getEvents(new protos.cockroach.server.serverpb.EventsRequest(req))
        .then((result) => {
          assert.lengthOf(fetchMock.calls(eventsPrefixMatcher), 1);
          assert.lengthOf(result.events, 1);
        });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);

      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: eventsPrefixMatcher,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api
        .getEvents(new protos.cockroach.server.serverpb.EventsRequest())
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(_.isError(e));
          done();
        });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: eventsPrefixMatcher,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => {});
        },
      });

      api
        .getEvents(
          new protos.cockroach.server.serverpb.EventsRequest(),
          moment.duration(0),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(
            _.startsWith(e.message, "Promise timed out"),
            "Error is a timeout error.",
          );
          done();
        });
    });
  });

  describe("health request", function () {
    const healthUrl = `${api.API_PREFIX}/health`;

    afterEach(fetchMock.restore);

    it("correctly requests health", function () {
      this.timeout(1000);
      // Mock out the fetch query
      fetchMock.mock({
        matcher: healthUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          const encodedResponse = protos.cockroach.server.serverpb.HealthResponse.encode(
            {},
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      return api
        .getHealth(new protos.cockroach.server.serverpb.HealthRequest())
        .then((result) => {
          assert.lengthOf(fetchMock.calls(healthUrl), 1);
          assert.deepEqual(
            result,
            new protos.cockroach.server.serverpb.HealthResponse(),
          );
        });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);

      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: healthUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api
        .getHealth(new protos.cockroach.server.serverpb.HealthRequest())
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(_.isError(e));
          done();
        });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: healthUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => {});
        },
      });

      api
        .getHealth(
          new protos.cockroach.server.serverpb.HealthRequest(),
          moment.duration(0),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(
            _.startsWith(e.message, "Promise timed out"),
            "Error is a timeout error.",
          );
          done();
        });
    });
  });

  describe("cluster request", function () {
    const clusterUrl = `${api.API_PREFIX}/cluster`;
    const clusterID = "12345abcde";

    afterEach(fetchMock.restore);

    it("correctly requests cluster info", function () {
      this.timeout(1000);
      fetchMock.mock({
        matcher: clusterUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          const encodedResponse = protos.cockroach.server.serverpb.ClusterResponse.encode(
            { cluster_id: clusterID },
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      return api
        .getCluster(new protos.cockroach.server.serverpb.ClusterRequest())
        .then((result) => {
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
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api
        .getCluster(new protos.cockroach.server.serverpb.ClusterRequest())
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(_.isError(e));
          done();
        });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: clusterUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => {});
        },
      });

      api
        .getCluster(
          new protos.cockroach.server.serverpb.ClusterRequest(),
          moment.duration(0),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(
            _.startsWith(e.message, "Promise timed out"),
            "Error is a timeout error.",
          );
          done();
        });
    });
  });

  describe("metrics metadata request", function () {
    const metricMetadataUrl = `${api.API_PREFIX}/metricmetadata`;
    afterEach(fetchMock.restore);

    it("returns list of metadata metrics", () => {
      this.timeout(1000);
      const metadata = {};
      fetchMock.mock({
        matcher: metricMetadataUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          const encodedResponse = protos.cockroach.server.serverpb.MetricMetadataResponse.encode(
            { metadata },
          ).finish();
          return {
            body: api.toArrayBuffer(encodedResponse),
          };
        },
      });

      return api
        .getAllMetricMetadata(
          new protos.cockroach.server.serverpb.MetricMetadataRequest(),
        )
        .then((result) => {
          assert.lengthOf(fetchMock.calls(metricMetadataUrl), 1);
          assert.deepEqual(result.metadata, metadata);
        });
    });

    it("correctly handles an error", function (done) {
      this.timeout(1000);

      // Mock out the fetch query, but return an error
      fetchMock.mock({
        matcher: metricMetadataUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return { throws: new Error() };
        },
      });

      api
        .getAllMetricMetadata(
          new protos.cockroach.server.serverpb.MetricMetadataRequest(),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(_.isError(e));
          done();
        });
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: metricMetadataUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => {});
        },
      });

      api
        .getAllMetricMetadata(
          new protos.cockroach.server.serverpb.MetricMetadataRequest(),
          moment.duration(0),
        )
        .then((_result) => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          assert(
            _.startsWith(e.message, "Promise timed out"),
            "Error is a timeout error.",
          );
          done();
        });
    });
  });

  describe("logs request", function () {
    const nodeId = "1";
    const logsUrl = `${api.STATUS_PREFIX}/logs/${nodeId}`;

    afterEach(fetchMock.restore);

    it("correctly requests log entries", function () {
      this.timeout(1000);
      const logEntry = {
        file: "f",
        goroutine: Long.fromNumber(1),
        message: "m",
        severity: Severity.ERROR,
      };
      fetchMock.mock({
        matcher: logsUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          const logsResponse = protos.cockroach.server.serverpb.LogEntriesResponse.encode(
            { entries: [logEntry] },
          ).finish();
          return {
            body: api.toArrayBuffer(logsResponse),
          };
        },
      });

      return api
        .getLogs(
          new protos.cockroach.server.serverpb.LogsRequest({ node_id: nodeId }),
        )
        .then((result) => {
          assert.lengthOf(fetchMock.calls(logsUrl), 1);
          assert.equal(result.entries.length, 1);
          assert.equal(result.entries[0].message, logEntry.message);
          assert.equal(result.entries[0].severity, logEntry.severity);
          assert.equal(result.entries[0].file, logEntry.file);
        });
    });

    it("correctly handles restricted permissions for remote debugging", function (done) {
      this.timeout(1000);
      fetchMock.mock({
        matcher: logsUrl,
        method: "GET",
        response: (_url: string) => {
          return {
            throws: new Error(
              "not allowed (due to the 'server.remote_debugging.mode' setting)",
            ),
          };
        },
      });

      api
        .getLogs(
          new protos.cockroach.server.serverpb.LogsRequest({ node_id: nodeId }),
        )
        .then((_result) => {
          assert.fail("Request unexpectedly succeeded.");
        })
        .catch(function (e: Error) {
          assert(_.isError(e));
          assert.equal(e.message, REMOTE_DEBUGGING_ERROR_TEXT);
        })
        .finally(done);
    });

    it("correctly times out", function (done) {
      this.timeout(1000);
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: logsUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          assert.isUndefined(requestObj.body);
          return new Promise<any>(() => {});
        },
      });

      api
        .getLogs(
          new protos.cockroach.server.serverpb.LogsRequest({ node_id: nodeId }),
          moment.duration(0),
        )
        .then((_result) => {
          assert.fail("Request unexpectedly succeeded.");
        })
        .catch(function (e) {
          assert(
            _.startsWith(e.message, "Promise timed out"),
            "Error is a timeout error.",
          );
        })
        .finally(done);
    });
  });
});
