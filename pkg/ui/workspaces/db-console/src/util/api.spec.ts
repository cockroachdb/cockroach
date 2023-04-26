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
import moment from "moment-timezone";
import Long from "long";

import fetchMock from "./fetch-mock";

import * as protos from "@cockroachlabs/crdb-protobuf-client";
const cockroach = protos.cockroach;
import * as api from "./api";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import { REMOTE_DEBUGGING_ERROR_TEXT } from "src/util/constants";
import Severity = protos.cockroach.util.log.Severity;
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
    const mockZoneConfigBytes: Buffer | Uint8Array =
      ZoneConfig.encode(mockZoneConfig).finish();
    const mockZoneConfigHexString = Array.from(mockZoneConfigBytes)
      .map(x => x.toString(16).padStart(2, "0"))
      .join("");

    afterEach(fetchMock.restore);

    it("correctly requests info about a specific database", function () {
      // Mock out the fetch query
      stubSqlApiCall<clusterUiApi.DatabaseDetailsRow>(
        clusterUiApi.createDatabaseDetailsReq(dbName),
        [
          // Database ID query
          { rows: [{ database_id: "1" }] },
          // Database grants query
          {
            rows: [
              {
                user: "admin",
                privileges: ["ALL"],
              },
              {
                user: "public",
                privileges: ["CONNECT"],
              },
            ],
          },
          // Database tables query
          {
            rows: [{ table_schema: "public", table_name: "table1" }],
          },
          // Database replicas and regions query
          {
            rows: [
              {
                replicas: [1, 2, 3],
                regions: ["gcp-europe-west1", "gcp-europe-west2"],
              },
            ],
          },
          // Database index usage statistics query
          {
            rows: [
              {
                last_read: mockOldDate.toISOString(),
                created_at: mockOldDate.toISOString(),
                unused_threshold: "1m",
              },
            ],
          },
          // Database zone config query
          {
            rows: [
              {
                zone_config_hex_string: mockZoneConfigHexString,
              },
            ],
          },
          // Database span stats query
          {
            rows: [
              {
                approximate_disk_bytes: 100,
                live_bytes: 200,
                total_bytes: 300,
                range_count: 400,
              },
            ],
          },
        ],
      );

      return clusterUiApi.getDatabaseDetails(dbName).then(result => {
        expect(fetchMock.calls(clusterUiApi.SQL_API_PATH).length).toBe(1);
        expect(result.results.idResp.database_id).toEqual("1");
        expect(result.results.tablesResp.tables.length).toBe(1);
        expect(result.results.grantsResp.grants.length).toBe(2);
        expect(result.results.stats.indexStats.num_index_recommendations).toBe(
          1,
        );
        expect(result.results.zoneConfigResp.zone_config).toEqual(
          mockZoneConfig,
        );
        expect(result.results.zoneConfigResp.zone_config_level).toBe(
          ZoneConfigurationLevel.DATABASE,
        );
        expect(result.results.stats.spanStats.approximate_disk_bytes).toBe(100);
        expect(result.results.stats.spanStats.live_bytes).toBe(200);
        expect(result.results.stats.spanStats.total_bytes).toBe(300);
        expect(result.results.stats.spanStats.range_count).toBe(400);
      });
    });

    it("correctly handles an error", function (done) {
      // Mock out the fetch query, but return a 500 status code
      const req = clusterUiApi.createDatabaseDetailsReq(dbName);
      fetchMock.mock({
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...req,
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
            database: req.database || clusterUiApi.FALLBACK_DB,
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
      const req = clusterUiApi.createDatabaseDetailsReq(dbName);
      fetchMock.mock({
        matcher: clusterUiApi.SQL_API_PATH,
        method: "POST",
        response: (_url: string, requestObj: RequestInit) => {
          expect(JSON.parse(requestObj.body.toString())).toEqual({
            ...req,
            application_name: clusterUiApi.INTERNAL_SQL_API_APP,
            database: req.database || clusterUiApi.FALLBACK_DB,
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
    const mockZoneConfigBytes: Buffer | Uint8Array =
      ZoneConfig.encode(mockZoneConfig).finish();
    const mockZoneConfigHexString = Array.from(mockZoneConfigBytes)
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
          { rows: [{ create_statement: "mock create stmt" }] },
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
                database_zone_config_hex_string: mockZoneConfigHexString,
                table_zone_config_hex_string: null,
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
          expect(resp.results.idResp.table_id).toBe("1");
          expect(resp.results.grantsResp.grants.length).toBe(1);
          expect(resp.results.schemaDetails.columns.length).toBe(3);
          expect(resp.results.schemaDetails.indexes.length).toBe(2);
          expect(resp.results.createStmtResp.create_statement).toBe(
            "mock create stmt",
          );
          expect(resp.results.zoneConfigResp.configure_zone_statement).toBe(
            "mock zone config stmt",
          );
          expect(
            moment(resp.results.heuristicsDetails.stats_last_created_at).isSame(
              mockStatsLastCreatedTimestamp,
            ),
          ).toBe(true);
          expect(resp.results.stats.spanStats.approximate_disk_bytes).toBe(100);
          expect(resp.results.stats.spanStats.live_bytes).toBe(200);
          expect(resp.results.stats.spanStats.total_bytes).toBe(400);
          expect(resp.results.stats.spanStats.range_count).toBe(400);
          expect(resp.results.stats.spanStats.live_percentage).toBe(0.5);
          expect(resp.results.stats.indexStats.has_index_recommendations).toBe(
            true,
          );
          expect(resp.results.zoneConfigResp.zone_config).toEqual(
            mockZoneConfig,
          );
          expect(resp.results.zoneConfigResp.zone_config_level).toBe(
            ZoneConfigurationLevel.DATABASE,
          );
          expect(resp.results.stats.replicaData.replicaCount).toBe(3);
          expect(resp.results.stats.replicaData.nodeCount).toBe(3);
          expect(resp.results.stats.replicaData.nodeIDs).toEqual([1, 2, 3]);
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
            database: clusterUiApi.FALLBACK_DB,
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
            database: clusterUiApi.FALLBACK_DB,
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
            database: clusterUiApi.FALLBACK_DB,
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
            database: clusterUiApi.FALLBACK_DB,
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
          expect(result).toEqual(
            new protos.cockroach.server.serverpb.HealthResponse(),
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
