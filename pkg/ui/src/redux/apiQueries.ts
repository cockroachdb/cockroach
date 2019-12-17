// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";
import moment from "moment";
import { call, put } from "redux-saga/effects";
import { hashHistory } from "react-router";
import { push } from "react-router-redux";

import { CachedQuery, keyedQuery } from "./cachedQuery";
import { getLoginPage } from "src/redux/login";
import * as api from "src/util/api";
import { versionCheck } from "src/util/cockroachlabsAPI";
import { INodeStatus, RollupStoreMetrics } from "src/util/proto";
import * as protos from "src/js/protos";

// Simple API Queries with default settings.

export const clusterQuery = new CachedQuery("cluster", () => api.getCluster(null));
export const eventsQuery = new CachedQuery("events", () => api.getEvents(null));
export const raftQuery = new CachedQuery("raft", () => api.raftDebug(null));
export const versionQuery = new CachedQuery("version", () => versionCheck(null));
export const locationsQuery = new CachedQuery("locations", () => api.getLocations(null));
export const databasesQuery = new CachedQuery("databases", () => api.getDatabaseList(null));
export const nonTableStatsQuery = new CachedQuery("nonTableStats", () => api.getNonTableStats(null));
export const livenessQuery = new CachedQuery("liveness", () => api.getLiveness(null));

// The health query, executed every 2 seconds as a sort of canary check, is
// also responsible for detecting unauthorized errors and redirecting to the
// login page.
export const healthQuery = new CachedQuery<protos.cockroach.server.serverpb.HealthResponse>(
  "health",
  function *(): any {
    try {
      return yield call(api.getHealth, null);
    } catch (e) {
      if (e instanceof Error) {
        if (e.message === "Unauthorized") {
          const location = hashHistory.getCurrentLocation();
          if (location && !location.pathname.startsWith("/login")) {
                put(push(getLoginPage(location)));
          }
        }
      }
      // Rethrow error to query manager.
      throw e;
    }
  },
  {
    refreshInterval: moment.duration(2, "s"),
  },
);

// Nodes query processes the raw response from the server before caching.
export const nodesQuery = new CachedQuery<INodeStatus[]>("nodes", function *() {
  const response: api.NodesResponseMessage = yield api.getNodes(null);
  return _.map(response.nodes, (n) => {
    RollupStoreMetrics(n);
    return n;
  });
});

// Queries with non-default timing options.

export const settingsQuery = new CachedQuery(
  "settings",
  () => api.getSettings(null, moment.duration(1, "m")),
);

export const statementsQuery = new CachedQuery(
  "statements",
  () => api.getStatements(moment.duration(1, "m")),
  {
    refreshInterval: moment.duration(5, "m"),
  },
);

export const dataDistributionQuery = new CachedQuery(
  "dataDistribution",
  () => api.getDataDistribution(),
  {
    refreshInterval: moment.duration(2, "m"),
  },
);

export function databaseDetailsQuery(dbName: string) {
  return keyedQuery(
    "databaseDetails." + dbName,
    () => {
      const request = new protos.cockroach.server.serverpb.DatabaseDetailsRequest({
        database: dbName,
      });
      return api.getDatabaseDetails(request);
    },
  );
}

export function tableDetailsQuery(dbName: string, tblName: string) {
  return keyedQuery(
    `tableDetails.${encodeURIComponent(dbName)}.${encodeURIComponent(tblName)}`,
    () => {
      const request = new protos.cockroach.server.serverpb.TableDetailsRequest({
        database: dbName,
        table: tblName,
      });
      return api.getTableDetails(request);
    },
  );
}

export function tableStatsQuery(dbName: string, tblName: string) {
  return keyedQuery(
    `tableStats.${encodeURIComponent(dbName)}.${encodeURIComponent(tblName)}`,
    () => {
      const request = new protos.cockroach.server.serverpb.TableStatsRequest({
        database: dbName,
        table: tblName,
      });
      api.getTableStats(request);
    },
  );
}

export const jobsKey = (status: string, type: protos.cockroach.sql.jobs.jobspb.Type, limit: number) =>
  `${encodeURIComponent(status)}/${encodeURIComponent(type.toString())}/${encodeURIComponent(limit.toString())}`;

export function jobsQuery(request: api.JobsRequestMessage) {
  return keyedQuery(
    "jobs." + jobsKey(request.status, request.type, request.limit),
    () => api.getJobs(request),
  );
}

export function problemRangesQuery(nodeID: string) {
  return keyedQuery(
    "problemRanges." + _.isEmpty(nodeID) ? "all" : nodeID,
    () => {
      const req = new protos.cockroach.server.serverpb.ProblemRangesRequest({
        node_id: nodeID,
      });
      return api.getProblemRanges(req, moment.duration(1, "m"));
    },
  );
}

export function certificatesQuery(req: api.CertificatesRequestMessage) {
  return keyedQuery(
    "certificates." + _.isEmpty(req.node_id) ? "none" : req.node_id,
    () => api.getCertificates(req),
    {
      refreshInterval: moment.duration(1, "m"),
    },
  );
}

export function rangeQuery(req: api.RangeRequestMessage) {
  return keyedQuery(
    "range." + _.isNil(req.range_id) ? "none" : req.range_id.toString(),
    () => api.getRange(req, moment.duration(1, "m")),
  );
}

export function allocatorRangeQuery(req: api.AllocatorRangeRequestMessage) {
  return keyedQuery(
    "allocatorRange." + _.isNil(req.range_id) ? "none" : req.range_id.toString(),
    () => api.getAllocatorRange(req, moment.duration(1, "m")),
  );
}

export function rangeLogQuery(req: api.RangeLogRequestMessage) {
  return keyedQuery(
    "rangeLog." + _.isNil(req.range_id) ? "none" : req.range_id.toString(),
    () => api.getRangeLog(req, moment.duration(5, "m")),
  );
}

export function commandQueueQuery(req: api.CommandQueueRequestMessage) {
  return keyedQuery(
    "commandQueue." + _.isNil(req.range_id) ? "none" : req.range_id.toString(),
    () => api.getCommandQueue(req, moment.duration(1, "m")),
  );
}

export function storesQuery(req: api.StoresRequestMessage) {
  return keyedQuery(
    "stores." + _.isNil(req.node_id) ? "none" : req.node_id.toString(),
    () => api.getStores(req, moment.duration(1, "m")),
  );
}
