// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
  describe("databases request", function () {
    afterEach(fetchMock.restore);

    it("correctly requests info about all databases", function () {
      // Mock out the fetch query to /databases
      fetchMock.mock({
        matcher: api.API_PREFIX + "/databases",
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          const encodedResponse =
            protos.cockroach.server.serverpb.DatabasesResponse.encode({
              databases: ["system", "test"],
            }).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      return api
        .getDatabaseList(
          new protos.cockroach.server.serverpb.DatabasesRequest(),
        )
        .then(result => {
          expect(fetchMock.calls(api.API_PREFIX + "/databases").length).toBe(1);
          expect(result.databases.length).toBe(2);
        });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query to /databases, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: api.API_PREFIX + "/databases",
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return { throws: new Error() };
        },
      });

      api
        .getDatabaseList(
          new protos.cockroach.server.serverpb.DatabasesRequest(),
        )
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.isError(e)).toBeTruthy();
          done();
        });
    });

    it("correctly times out", function (done) {
      // Mock out the fetch query to /databases, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: api.API_PREFIX + "/databases",
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return new Promise<any>(() => {});
        },
      });

      api
        .getDatabaseList(
          new protos.cockroach.server.serverpb.DatabasesRequest(),
          moment.duration(0),
        )
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.startsWith(e.message, "Promise timed out")).toBeTruthy();
          done();
        });
    });
  });

  describe("database details request", function () {
    const dbName = "test";

    afterEach(fetchMock.restore);

    it("correctly requests info about a specific database", function () {
      // Mock out the fetch query
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          const encodedResponse =
            protos.cockroach.server.serverpb.DatabaseDetailsResponse.encode({
              table_names: ["table1", "table2"],
              grants: [
                { user: "root", privileges: ["ALL"] },
                { user: "other", privileges: [] },
              ],
            }).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      return api
        .getDatabaseDetails(
          new protos.cockroach.server.serverpb.DatabaseDetailsRequest({
            database: dbName,
          }),
        )
        .then(result => {
          expect(
            fetchMock.calls(`${api.API_PREFIX}/databases/${dbName}`).length,
          ).toBe(1);
          expect(result.table_names.length).toBe(2);
          expect(result.grants.length).toBe(2);
        });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return { throws: new Error() };
        },
      });

      api
        .getDatabaseDetails(
          new protos.cockroach.server.serverpb.DatabaseDetailsRequest({
            database: dbName,
          }),
        )
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.isError(e)).toBeTruthy();
          done();
        });
    });

    it("correctly times out", function (done) {
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
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
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.startsWith(e.message, "Promise timed out")).toBeTruthy();
          done();
        });
    });
  });

  describe("table details request", function () {
    const dbName = "testDB";
    const tableName = "testTable";

    afterEach(fetchMock.restore);

    it("correctly requests info about a specific table", function () {
      // Mock out the fetch query
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          const encodedResponse =
            protos.cockroach.server.serverpb.TableDetailsResponse.encode(
              {},
            ).finish();
          return {
            body: encodedResponse,
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
        .then(result => {
          expect(
            fetchMock.calls(
              `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
            ).length,
          ).toBe(1);
          expect(result.columns.length).toBe(0);
          expect(result.indexes.length).toBe(0);
          expect(result.grants.length).toBe(0);
        });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
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
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.isError(e)).toBeTruthy();
          done();
        });
    });

    it("correctly times out", function (done) {
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: `${api.API_PREFIX}/databases/${dbName}/tables/${tableName}`,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
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
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.startsWith(e.message, "Promise timed out")).toBeTruthy();
          done();
        });
    });
  });

  describe("events request", function () {
    const eventsPrefixMatcher = `begin:${api.API_PREFIX}/events?`;

    afterEach(fetchMock.restore);

    it("correctly requests events", function () {
      // Mock out the fetch query
      fetchMock.mock({
        matcher: eventsPrefixMatcher,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          const encodedResponse =
            protos.cockroach.server.serverpb.EventsResponse.encode({
              events: [{ event_type: "test" }],
            }).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      return api
        .getEvents(new protos.cockroach.server.serverpb.EventsRequest())
        .then(result => {
          expect(fetchMock.calls(eventsPrefixMatcher).length).toBe(1);
          expect(result.events.length).toBe(1);
        });
    });

    it("correctly requests filtered events", function () {
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
          expect(params.length).toBe(3);
          _.each(params, param => {
            let [k, v] = param.split("=");
            k = decodeURIComponent(k);
            v = decodeURIComponent(v);
            switch (k) {
              case "target_id":
                expect(req.target_id.toString()).toEqual(v);
                break;

              case "type":
                expect(req.type).toEqual(v);
                break;

              case "unredacted_events":
                break;

              default:
                throw new Error(`Unknown property ${k}`);
            }
          });
          expect(requestObj.body).toBeUndefined();
          const encodedResponse =
            protos.cockroach.server.serverpb.EventsResponse.encode({
              events: [{ event_type: "test" }],
            }).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      return api
        .getEvents(new protos.cockroach.server.serverpb.EventsRequest(req))
        .then(result => {
          expect(fetchMock.calls(eventsPrefixMatcher).length).toBe(1);
          expect(result.events.length).toBe(1);
        });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: eventsPrefixMatcher,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return { throws: new Error() };
        },
      });

      api
        .getEvents(new protos.cockroach.server.serverpb.EventsRequest())
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.isError(e)).toBeTruthy();
          done();
        });
    });

    it("correctly times out", function (done) {
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: eventsPrefixMatcher,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return new Promise<any>(() => {});
        },
      });

      api
        .getEvents(
          new protos.cockroach.server.serverpb.EventsRequest(),
          moment.duration(0),
        )
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.startsWith(e.message, "Promise timed out")).toBeTruthy();
          done();
        });
    });
  });

  describe("health request", function () {
    const healthUrl = `${api.API_PREFIX}/health`;

    afterEach(fetchMock.restore);

    it("correctly requests health", function () {
      // Mock out the fetch query
      fetchMock.mock({
        matcher: healthUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          const encodedResponse =
            protos.cockroach.server.serverpb.HealthResponse.encode({}).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      return api
        .getHealth(new protos.cockroach.server.serverpb.HealthRequest())
        .then(result => {
          expect(fetchMock.calls(healthUrl).length).toBe(1);
          expect(result.toJSON()).toEqual(
            new protos.cockroach.server.serverpb.HealthResponse().toJSON(),
          );
        });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: healthUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return { throws: new Error() };
        },
      });

      api
        .getHealth(new protos.cockroach.server.serverpb.HealthRequest())
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.isError(e)).toBeTruthy();
          done();
        });
    });

    it("correctly times out", function (done) {
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: healthUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return new Promise<any>(() => {});
        },
      });

      api
        .getHealth(
          new protos.cockroach.server.serverpb.HealthRequest(),
          moment.duration(0),
        )
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.startsWith(e.message, "Promise timed out")).toBeTruthy();
          done();
        });
    });
  });

  describe("cluster request", function () {
    const clusterUrl = `${api.API_PREFIX}/cluster`;
    const clusterID = "12345abcde";

    afterEach(fetchMock.restore);

    it("correctly requests cluster info", function () {
      fetchMock.mock({
        matcher: clusterUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          const encodedResponse =
            protos.cockroach.server.serverpb.ClusterResponse.encode({
              cluster_id: clusterID,
            }).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      return api
        .getCluster(new protos.cockroach.server.serverpb.ClusterRequest())
        .then(result => {
          expect(fetchMock.calls(clusterUrl).length).toBe(1);
          expect(result.cluster_id).toEqual(clusterID);
        });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return an error
      fetchMock.mock({
        matcher: clusterUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return { throws: new Error() };
        },
      });

      api
        .getCluster(new protos.cockroach.server.serverpb.ClusterRequest())
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.isError(e)).toBeTruthy();
          done();
        });
    });

    it("correctly times out", function (done) {
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: clusterUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return new Promise<any>(() => {});
        },
      });

      api
        .getCluster(
          new protos.cockroach.server.serverpb.ClusterRequest(),
          moment.duration(0),
        )
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.startsWith(e.message, "Promise timed out")).toBeTruthy();
          done();
        });
    });
  });

  describe("metrics metadata request", function () {
    const metricMetadataUrl = `${api.API_PREFIX}/metricmetadata`;
    afterEach(fetchMock.restore);

    it("returns list of metadata metrics", () => {
      const metadata = {};
      fetchMock.mock({
        matcher: metricMetadataUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          const encodedResponse =
            protos.cockroach.server.serverpb.MetricMetadataResponse.encode({
              metadata,
            }).finish();
          return {
            body: encodedResponse,
          };
        },
      });

      return api
        .getAllMetricMetadata(
          new protos.cockroach.server.serverpb.MetricMetadataRequest(),
        )
        .then(result => {
          expect(fetchMock.calls(metricMetadataUrl).length).toBe(1);
          expect(result.metadata).toEqual(metadata);
        });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return an error
      fetchMock.mock({
        matcher: metricMetadataUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return { throws: new Error() };
        },
      });

      api
        .getAllMetricMetadata(
          new protos.cockroach.server.serverpb.MetricMetadataRequest(),
        )
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.isError(e)).toBeTruthy();
          done();
        });
    });

    it("correctly times out", function (done) {
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: metricMetadataUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return new Promise<any>(() => {});
        },
      });

      api
        .getAllMetricMetadata(
          new protos.cockroach.server.serverpb.MetricMetadataRequest(),
          moment.duration(0),
        )
        .then(_result => {
          done(new Error("Request unexpectedly succeeded."));
        })
        .catch(function (e) {
          expect(_.startsWith(e.message, "Promise timed out")).toBeTruthy();
          done();
        });
    });
  });

  describe("logs request", function () {
    const nodeId = "1";
    const logsUrl = `${api.STATUS_PREFIX}/logs/${nodeId}`;

    afterEach(fetchMock.restore);

    it("correctly requests log entries", function () {
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
          expect(requestObj.body).toBeUndefined();
          const logsResponse =
            protos.cockroach.server.serverpb.LogEntriesResponse.encode({
              entries: [logEntry],
            }).finish();
          return {
            body: logsResponse,
          };
        },
      });

      return api
        .getLogs(
          new protos.cockroach.server.serverpb.LogsRequest({ node_id: nodeId }),
        )
        .then(result => {
          expect(fetchMock.calls(logsUrl).length).toBe(1);
          expect(result.entries.length).toBe(1);
          expect(result.entries[0].message).toBe(logEntry.message);
          expect(result.entries[0].severity).toEqual(logEntry.severity);
          expect(result.entries[0].file).toBe(logEntry.file);
        });
    });

    it("correctly handles restricted permissions for remote debugging", function (done) {
      fetchMock.mock({
        matcher: logsUrl,
        method: "GET",
        response: (_url: string) => {
          return {
            throws: new Error(REMOTE_DEBUGGING_ERROR_TEXT),
          };
        },
      });

      api
        .getLogs(
          new protos.cockroach.server.serverpb.LogsRequest({ node_id: nodeId }),
        )
        .then(_result => {
          expect(false).toBe(true);
        })
        .catch(function (e: Error) {
          expect(_.isError(e)).toBeTruthy();
          expect(e.message).toBe(REMOTE_DEBUGGING_ERROR_TEXT);
        })
        .finally(done);
    });

    it("correctly times out", function (done) {
      // Mock out the fetch query, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: logsUrl,
        method: "GET",
        response: (_url: string, requestObj: RequestInit) => {
          expect(requestObj.body).toBeUndefined();
          return new Promise<any>(() => {});
        },
      });

      api
        .getLogs(
          new protos.cockroach.server.serverpb.LogsRequest({ node_id: nodeId }),
          moment.duration(0),
        )
        .then(_result => {
          expect(false).toBe(true);
        })
        .catch(function (e) {
          expect(_.startsWith(e.message, "Promise timed out")).toBeTruthy();
        })
        .finally(done);
    });
  });
});
