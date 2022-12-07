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
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { REMOTE_DEBUGGING_ERROR_TEXT } from "src/util/constants";
import Severity = cockroach.util.log.Severity;
import {
  buildSQLApiDatabasesResponse,
  buildSQLApiEventsResponse,
  buildSqlExecutionResponse,
} from "src/util/fakeApi";

describe("rest api", function () {
  describe("databases request", function () {
    afterEach(fetchMock.restore);

    it("correctly requests info about all databases", function () {
      // Mock out the fetch query to /databases
      fetchMock.mock({
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "X-Cockroach-API-Session": "cookie",
        },
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual(
            clusterUiApi.databasesRequest,
          );
          return {
            body: JSON.stringify(
              buildSQLApiDatabasesResponse(["system", "test"]),
            ),
          };
        },
      });

      return clusterUiApi.getDatabasesList().then(result => {
        expect(fetchMock.calls(clusterUiApi.SQL_API_PATH).length).toBe(1);
        expect(result.databases.length).toBe(2);
      });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query to /databases, but return a promise that's never resolved to test the timeout
      fetchMock.mock({
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual(
            clusterUiApi.databasesRequest,
          );
          return { throws: new Error() };
        },
      });

      clusterUiApi
        .getDatabasesList()
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
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual(
            clusterUiApi.databasesRequest,
          );
          return new Promise<any>(() => {});
        },
      });

      clusterUiApi
        .getDatabasesList(moment.duration(0))
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
        headers: {
          Accept: "application/json",
          "Content-Type": "application/json",
          "X-Cockroach-API-Session": "cookie",
        },
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...clusterUiApi.createDatabaseDetailsReq(dbName),
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
          });
          const mockResp =
            buildSqlExecutionResponse<clusterUiApi.DatabaseDetailsRow>([
              // Database ID query
              { rows: [{ database_id: "1" }] },
              // Database grants query
              {
                rows: [
                  {
                    database_name: "test",
                    grantee: "admin",
                    privilege_type: "ALL",
                    is_grantable: true,
                  },
                  {
                    database_name: "test",
                    grantee: "public",
                    privilege_type: "CONNECT",
                    is_grantable: false,
                  },
                ],
              },
              // Database tables query
              {
                rows: [{ table_schema: "public", table_name: "table1" }],
              },
              // Database ranges query
              {
                rows: [
                  {
                    range_id: 1,
                    table_id: 1,
                    database_name: "test",
                    schema_name: "public",
                    table_name: "table1",
                    replicas: [1, 2, 3],
                    regions: ["gcp-europe-west1", "gcp-europe-west2"],
                    range_size: 125,
                  },
                ],
              },
              // Database index usage statistics query
              {
                rows: [
                  {
                    database_name: "test",
                    table_name: "table1",
                    table_id: 1,
                    index_name: "test_idx",
                    index_id: 1,
                    index_type: "primary",
                    total_reads: 12,
                    last_read: new Date().toISOString(),
                    created_at: new Date().toISOString(),
                    unused_threshold: "30m",
                  },
                ],
              },
            ]);

          return {
            body: JSON.stringify(mockResp),
          };
        },
      });

      return clusterUiApi.getDatabaseDetails(dbName).then(result => {
        expect(fetchMock.calls(clusterUiApi.SQL_API_PATH).length).toBe(1);
        expect(result.id_resp.id.database_id).toEqual("1");
        expect(result.tables_resp.tables.length).toBe(1);
        expect(result.grants_resp.grants.length).toBe(2);
        expect(result.stats.ranges_data.count).toBe(1);
        expect(result.stats.index_stats.num_index_recommendations).toBe(0);
      });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...clusterUiApi.createDatabaseDetailsReq(dbName),
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
          });
          return { throws: new Error() };
        },
      });

      clusterUiApi
        .getDatabaseDetails(dbName)
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
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...clusterUiApi.createDatabaseDetailsReq(dbName),
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
          });
          return new Promise<any>(() => {});
        },
      });

      clusterUiApi
        .getDatabaseDetails(dbName, moment.duration(0))
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
    afterEach(fetchMock.restore);

    it("correctly requests events", function () {
      // Mock out the fetch query
      fetchMock.mock({
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...clusterUiApi.buildEventsSQLRequest({}),
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
          });
          return {
            body: JSON.stringify(
              buildSQLApiEventsResponse([
                {
                  eventType: "test",
                  timestamp: "2016-01-25T10:10:10.555555",
                  reportingID: "1",
                  info: `{"Timestamp":1668442242840943000,"EventType":"test","NodeID":1,"StartedAt":1668442242644228000,"LastUp":1668442242644228000}`,
                  uniqueID: "\\\x4ce0d9e74bd5480ab1d9e6f98cc2f483",
                },
              ]),
            ),
          };
        },
      });

      return clusterUiApi.getNonRedactedEvents().then(result => {
        expect(fetchMock.calls(clusterUiApi.SQL_API_PATH).length).toBe(1);
        expect(result.length).toBe(1);
      });
    });

    it("correctly requests filtered events", function () {
      const req: clusterUiApi.NonRedactedEventsRequest = { type: "test" };

      // Mock out the fetch query
      fetchMock.mock({
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...clusterUiApi.buildEventsSQLRequest(req),
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
          });
          return {
            body: JSON.stringify(
              buildSQLApiEventsResponse([
                {
                  eventType: "test",
                  timestamp: "2016-01-25T10:10:10.555555",
                  reportingID: "1",
                  info: `{"Timestamp":1668442242840943000,"EventType":"test","NodeID":1,"StartedAt":1668442242644228000,"LastUp":1668442242644228000}`,
                  uniqueID: "\\\x4ce0d9e74bd5480ab1d9e6f98cc2f483",
                },
              ]),
            ),
          };
        },
      });

      return clusterUiApi.getNonRedactedEvents(req).then(result => {
        expect(fetchMock.calls(clusterUiApi.SQL_API_PATH).length).toBe(1);
        expect(result.length).toBe(1);
      });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...clusterUiApi.buildEventsSQLRequest({}),
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
          });
          return { throws: new Error() };
        },
      });

      clusterUiApi
        .getNonRedactedEvents()
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
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...clusterUiApi.buildEventsSQLRequest({}),
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
          });
          return new Promise<any>(() => {});
        },
      });

      clusterUiApi
        .getNonRedactedEvents({}, moment.duration(0))
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
