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
  stubSqlApiCall,
} from "src/util/fakeApi";

const { ZoneConfig } = cockroach.config.zonepb;
const { ZoneConfigurationLevel } = cockroach.server.serverpb;

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
    const mockOldDate = new Date(2023, 2, 3);
    const mockZoneConfig = new ZoneConfig({
      inherited_constraints: true,
      inherited_lease_preferences: true,
      null_voter_constraints_is_empty: true,
      global_reads: true,
      gc: {
        ttl_seconds: 100,
      },
    });
    const mockZoneConfigBytes = ZoneConfig.encode(mockZoneConfig).finish();
    // Convert bytes representation to hex string - format returned by API.
    const hexStringPrefix = "\\x";
    const mockZoneConfigHexString =
      hexStringPrefix +
      Array.from(mockZoneConfigBytes)
        .map(x => x.toString(16).padStart(2, "0"))
        .join("");
    const mockStatsLastCreatedTimestamp = moment();

    afterEach(fetchMock.restore);

    it("correctly requests info about a specific table", function () {
      // Mock out the fetch query
      stubSqlApiCall<clusterUiApi.TableDetailsRow>(
        clusterUiApi.createTableDetailsReq(dbName, tableName),
        [
          // Table ID query
          { rows: [{ table_id: "1" }] },
          // Table grants query
          {
            rows: [{ user: "user", privileges: ["ALL", "NONE", "PRIVILEGE"] }],
          },
          // Table schema details query
          { rows: [{ columns: ["a", "b", "c"], indexes: ["d", "e"] }] },
          // Table create statement query
          { rows: [{ statement: "mock create stmt" }] },
          // Table zone config statement query
          { rows: [{ raw_config_sql: "mock zone config stmt" }] },
          // Table heuristics query
          { rows: [{ stats_last_created_at: mockStatsLastCreatedTimestamp }] },
          // Table span stats query
          {
            rows: [
              {
                approximate_disk_bytes: 100,
                live_bytes: 200,
                total_bytes: 400,
                range_count: 400,
                live_percentage: 0.5,
              },
            ],
          },
          // Table index usage statistics query
          {
            rows: [
              {
                last_read: mockOldDate.toISOString(),
                created_at: mockOldDate.toISOString(),
                unused_threshold: "1m",
              },
            ],
          },
          // Table zone config query
          {
            rows: [
              {
                database_zone_config_bytes: mockZoneConfigHexString,
                table_zone_config_bytes: null,
              },
            ],
          },
          // Table replicas query
          {
            rows: [{ replicas: [1, 2, 3] }],
          },
        ],
      );

      return clusterUiApi
        .getTableDetails({
          database: dbName,
          table: tableName,
        })
        .then(resp => {
          expect(fetchMock.calls(clusterUiApi.SQL_API_PATH).length).toBe(1);
          expect(resp.results.id_resp.table_id).toBe("1");
          expect(resp.results.grants_resp.grants.length).toBe(1);
          expect(resp.results.schema_details.columns.length).toBe(3);
          expect(resp.results.schema_details.indexes.length).toBe(2);
          expect(resp.results.create_stmt_resp.statement).toBe(
            "mock create stmt",
          );
          expect(resp.results.zone_config_resp.configure_zone_statement).toBe(
            "mock zone config stmt",
          );
          expect(
            JSON.stringify(
              resp.results.heuristics_details.stats_last_created_at,
            ),
          ).toEqual(JSON.stringify(mockStatsLastCreatedTimestamp));
          expect(resp.results.stats.pebble_data.approximate_disk_bytes).toBe(
            100,
          );
          expect(resp.results.stats.ranges_data.live_bytes).toBe(200);
          expect(resp.results.stats.ranges_data.total_bytes).toBe(400);
          expect(resp.results.stats.ranges_data.range_count).toBe(400);
          expect(resp.results.stats.ranges_data.live_percentage).toBe(0.5);
          expect(resp.results.stats.index_stats.has_index_recommendations).toBe(
            true,
          );
          expect(resp.results.zone_config_resp.zone_config).toEqual(
            mockZoneConfig,
          );
          expect(resp.results.zone_config_resp.zone_config_level).toBe(
            ZoneConfigurationLevel.DATABASE,
          );
          expect(resp.results.stats.ranges_data.replica_count).toBe(3);
          expect(resp.results.stats.ranges_data.node_count).toBe(3);
          expect(resp.results.stats.ranges_data.node_ids).toEqual([1, 2, 3]);
        });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return a 500 status code
      fetchMock.mock({
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...clusterUiApi.createTableDetailsReq(dbName, tableName),
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
          });
          return { throws: new Error() };
        },
      });

      clusterUiApi
        .getTableDetails({
          database: dbName,
          table: tableName,
        })
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
            ...clusterUiApi.createTableDetailsReq(dbName, tableName),
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
          });
          return new Promise<any>(() => {});
        },
      });

      clusterUiApi
        .getTableDetails(
          {
            database: dbName,
            table: tableName,
          },
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
            database: "system",
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
        expect(result.results.length).toBe(1);
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
            database: "system",
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
        expect(result.results.length).toBe(1);
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
            database: "system",
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
            database: "system",
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
