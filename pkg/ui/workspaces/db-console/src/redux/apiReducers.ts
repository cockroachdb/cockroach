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
import { Action, combineReducers } from "redux";
import { ThunkAction, ThunkDispatch } from "redux-thunk";
import moment from "moment";
import {
  api as clusterUiApi,
  util,
  TxnContentionInsightDetails,
  TxnContentionInsightEvent,
  TxnInsightEvent,
} from "@cockroachlabs/cluster-ui";
import {
  CachedDataReducer,
  CachedDataReducerState,
  KeyedCachedDataReducer,
  KeyedCachedDataReducerState,
  PaginatedCachedDataReducer,
  PaginatedCachedDataReducerState,
} from "./cachedDataReducer";
import * as api from "src/util/api";
import { VersionList } from "src/interfaces/cockroachlabs";
import { versionCheck } from "src/util/cockroachlabsAPI";
import { INodeStatus, RollupStoreMetrics } from "src/util/proto";
import * as protos from "src/js/protos";
import Long from "long";

const { generateStmtDetailsToID } = util;

const SessionsRequest = protos.cockroach.server.serverpb.ListSessionsRequest;

// The primary export of this file are the "refresh" functions of the various
// reducers, which are used by many react components to request fresh data.
// However, some of the reducer objects are also fully exported for use
// in tests.

export const clusterReducerObj = new CachedDataReducer(
  api.getCluster,
  "cluster",
);
export const refreshCluster = clusterReducerObj.refresh;

const eventsReducerObj = new CachedDataReducer(
  clusterUiApi.getNonRedactedEvents,
  "events",
  moment.duration(10, "s"),
);
export const refreshEvents = eventsReducerObj.refresh;

export type HealthState = CachedDataReducerState<api.HealthResponseMessage>;
export const healthReducerObj = new CachedDataReducer(
  api.getHealth,
  "health",
  moment.duration(2, "s"),
);
export const refreshHealth = healthReducerObj.refresh;

function rollupStoreMetrics(
  res: api.NodesResponseExternalMessage,
): INodeStatus[] {
  return _.map(res.nodes, node => {
    RollupStoreMetrics(node);
    return node;
  });
}

export const nodesReducerObj = new CachedDataReducer(
  (req: api.NodesRequestMessage, timeout?: moment.Duration) =>
    api.getNodesUI(req, timeout).then(rollupStoreMetrics),
  "nodes",
  moment.duration(10, "s"),
);
export const refreshNodes = nodesReducerObj.refresh;

const raftReducerObj = new CachedDataReducer(
  api.raftDebug,
  "raft",
  moment.duration(10, "s"),
);
export const refreshRaft = raftReducerObj.refresh;

export const versionReducerObj = new CachedDataReducer(versionCheck, "version");
export const refreshVersion = versionReducerObj.refresh;

export const locationsReducerObj = new CachedDataReducer(
  api.getLocations,
  "locations",
  moment.duration(10, "m"),
);
export const refreshLocations = locationsReducerObj.refresh;

const databasesReducerObj = new CachedDataReducer(
  clusterUiApi.getDatabasesList,
  "databases",
  null,
  moment.duration(10, "m"),
);
export const refreshDatabases = databasesReducerObj.refresh;

export const databaseRequestToID = (
  req: api.DatabaseDetailsRequestMessage,
): string => req.database;

const databaseDetailsReducerObj = new KeyedCachedDataReducer(
  api.getDatabaseDetails,
  "databaseDetails",
  databaseRequestToID,
  null,
  moment.duration(10, "m"),
);

const hotRangesRequestToID = (req: api.HotRangesRequestMessage) =>
  req.page_token;

export const hotRangesReducerObj = new PaginatedCachedDataReducer(
  api.getHotRanges,
  "hotRanges",
  hotRangesRequestToID,
);

export const refreshDatabaseDetails = databaseDetailsReducerObj.refresh;

export const refreshHotRanges = hotRangesReducerObj.refresh;

// NOTE: We encode the db and table name so we can combine them as a string.
// TODO(maxlang): there's probably a nicer way to do this
export function generateTableID(db: string, table: string) {
  return `${encodeURIComponent(db)}/${encodeURIComponent(table)}`;
}

export const tableRequestToID = (
  req:
    | api.TableDetailsRequestMessage
    | api.TableStatsRequestMessage
    | api.IndexStatsRequestMessage,
): string => generateTableID(req.database, req.table);

const tableDetailsReducerObj = new KeyedCachedDataReducer(
  api.getTableDetails,
  "tableDetails",
  tableRequestToID,
  null,
  moment.duration(10, "m"),
);
export const refreshTableDetails = tableDetailsReducerObj.refresh;

const tableStatsReducerObj = new KeyedCachedDataReducer(
  api.getTableStats,
  "tableStats",
  tableRequestToID,
  null,
  moment.duration(10, "m"),
);
export const refreshTableStats = tableStatsReducerObj.refresh;

const indexStatsReducerObj = new KeyedCachedDataReducer(
  api.getIndexStats,
  "indexStats",
  tableRequestToID,
);

export const invalidateIndexStats =
  indexStatsReducerObj.cachedDataReducer.invalidateData;
export const refreshIndexStats = indexStatsReducerObj.refresh;

const nonTableStatsReducerObj = new CachedDataReducer(
  api.getNonTableStats,
  "nonTableStats",
  moment.duration(1, "m"),
);
export const refreshNonTableStats = nonTableStatsReducerObj.refresh;

const logsReducerObj = new CachedDataReducer(
  api.getLogs,
  "logs",
  moment.duration(10, "s"),
);
export const refreshLogs = logsReducerObj.refresh;

export const livenessReducerObj = new CachedDataReducer(
  api.getLiveness,
  "liveness",
  moment.duration(10, "s"),
);
export const refreshLiveness = livenessReducerObj.refresh;

export const jobsKey = (
  status: string,
  type: protos.cockroach.sql.jobs.jobspb.Type,
  limit: number,
) =>
  `${encodeURIComponent(status)}/${encodeURIComponent(
    type.toString(),
  )}/${encodeURIComponent(limit.toString())}`;

const jobsRequestKey = (req: api.JobsRequestMessage): string =>
  jobsKey(req.status, req.type, req.limit);

const jobsReducerObj = new KeyedCachedDataReducer(
  api.getJobs,
  "jobs",
  jobsRequestKey,
  null,
  moment.duration(10, "m"),
);
export const refreshJobs = jobsReducerObj.refresh;

export const jobRequestKey = (req: api.JobRequestMessage): string =>
  `${req.job_id}`;

const jobReducerObj = new KeyedCachedDataReducer(
  api.getJob,
  "job",
  jobRequestKey,
  null,
  moment.duration(10, "m"),
);
export const refreshJob = jobReducerObj.refresh;

export const queryToID = (req: api.QueryPlanRequestMessage): string =>
  req.query;

const queryPlanReducerObj = new CachedDataReducer(
  api.getQueryPlan,
  "queryPlan",
);
export const refreshQueryPlan = queryPlanReducerObj.refresh;

export const problemRangesRequestKey = (
  req: api.ProblemRangesRequestMessage,
): string => (_.isEmpty(req.node_id) ? "all" : req.node_id);

const problemRangesReducerObj = new KeyedCachedDataReducer(
  api.getProblemRanges,
  "problemRanges",
  problemRangesRequestKey,
  moment.duration(0),
  moment.duration(1, "m"),
);
export const refreshProblemRanges = problemRangesReducerObj.refresh;

export const certificatesRequestKey = (
  req: api.CertificatesRequestMessage,
): string => (_.isEmpty(req.node_id) ? "none" : req.node_id);

const certificatesReducerObj = new KeyedCachedDataReducer(
  api.getCertificates,
  "certificates",
  certificatesRequestKey,
  moment.duration(1, "m"),
);
export const refreshCertificates = certificatesReducerObj.refresh;

export const rangeRequestKey = (req: api.RangeRequestMessage): string =>
  _.isNil(req.range_id) ? "none" : req.range_id.toString();

const rangeReducerObj = new KeyedCachedDataReducer(
  api.getRange,
  "range",
  rangeRequestKey,
  moment.duration(0),
  moment.duration(1, "m"),
);
export const refreshRange = rangeReducerObj.refresh;

export const allocatorRangeRequestKey = (
  req: api.AllocatorRangeRequestMessage,
): string => (_.isNil(req.range_id) ? "none" : req.range_id.toString());

const allocatorRangeReducerObj = new KeyedCachedDataReducer(
  api.getAllocatorRange,
  "allocatorRange",
  allocatorRangeRequestKey,
  moment.duration(0),
  moment.duration(1, "m"),
);
export const refreshAllocatorRange = allocatorRangeReducerObj.refresh;

export const rangeLogRequestKey = (req: api.RangeLogRequestMessage): string =>
  _.isNil(req.range_id) ? "none" : req.range_id.toString();

const rangeLogReducerObj = new KeyedCachedDataReducer(
  api.getRangeLog,
  "rangeLog",
  rangeLogRequestKey,
  moment.duration(0),
  moment.duration(5, "m"),
);
export const refreshRangeLog = rangeLogReducerObj.refresh;

export const settingsReducerObj = new CachedDataReducer(
  api.getSettings,
  "settings",
  moment.duration(0),
  moment.duration(1, "m"),
);
export const refreshSettings = settingsReducerObj.refresh;

export const sessionsReducerObj = new CachedDataReducer(
  api.getSessions,
  "sessions",
  // The sessions page is polled at the usage sites.
  null,
  moment.duration(1, "m"),
);
export const invalidateSessions = sessionsReducerObj.invalidateData;
export const refreshSessions = sessionsReducerObj.refresh;

export const storesRequestKey = (req: api.StoresRequestMessage): string =>
  _.isEmpty(req.node_id) ? "none" : req.node_id;

const storesReducerObj = new KeyedCachedDataReducer(
  api.getStores,
  "stores",
  storesRequestKey,
  moment.duration(0),
  moment.duration(1, "m"),
);
export const refreshStores = storesReducerObj.refresh;

const queriesReducerObj = new CachedDataReducer(
  api.getCombinedStatements,
  "statements",
  null,
  moment.duration(30, "m"),
);
export const invalidateStatements = queriesReducerObj.invalidateData;
export const refreshStatements = queriesReducerObj.refresh;

export const statementDetailsRequestToID = (
  req: api.StatementDetailsRequestMessage,
): string =>
  generateStmtDetailsToID(
    req.fingerprint_id.toString(),
    req.app_names.toString(),
    req.start,
    req.end,
  );

export const statementDetailsActionNamespace = "statementDetails";
export const statementDetailsReducerObj = new KeyedCachedDataReducer(
  api.getStatementDetails,
  statementDetailsActionNamespace,
  statementDetailsRequestToID,
  moment.duration(5, "m"),
  moment.duration(30, "m"),
);

export const invalidateStatementDetails =
  statementDetailsReducerObj.cachedDataReducer.invalidateData;
export const invalidateAllStatementDetails =
  statementDetailsReducerObj.cachedDataReducer.invalidateAllData;
export const refreshStatementDetails = statementDetailsReducerObj.refresh;

const userSQLRolesReducerObj = new CachedDataReducer(
  api.getUserSQLRoles,
  "userSQLRoles",
  moment.duration(1, "m"),
);

export const invalidateUserSQLRoles = userSQLRolesReducerObj.invalidateData;
export const refreshUserSQLRoles = userSQLRolesReducerObj.refresh;

const statementDiagnosticsReportsReducerObj = new CachedDataReducer(
  clusterUiApi.getStatementDiagnosticsReports,
  "statementDiagnosticsReports",
  moment.duration(5, "m"),
  moment.duration(1, "m"),
);
export const refreshStatementDiagnosticsRequests =
  statementDiagnosticsReportsReducerObj.refresh;
export const invalidateStatementDiagnosticsRequests =
  statementDiagnosticsReportsReducerObj.invalidateData;

const dataDistributionReducerObj = new CachedDataReducer(
  api.getDataDistribution,
  "dataDistribution",
  moment.duration(1, "m"),
);
export const refreshDataDistribution = dataDistributionReducerObj.refresh;

const metricMetadataReducerObj = new CachedDataReducer(
  api.getAllMetricMetadata,
  "metricMetadata",
);
export const refreshMetricMetadata = metricMetadataReducerObj.refresh;

const clusterLocksReducerObj = new CachedDataReducer(
  clusterUiApi.getClusterLocksState,
  "clusterLocks",
  null,
  moment.duration(30, "s"),
);
export const refreshClusterLocks = clusterLocksReducerObj.refresh;

export const refreshLiveWorkload = (): ThunkAction<any, any, any, Action> => {
  return (dispatch: ThunkDispatch<unknown, unknown, Action>) => {
    dispatch(
      refreshSessions(new SessionsRequest({ exclude_closed_sessions: true })),
    );
    dispatch(refreshClusterLocks());
  };
};

const transactionInsightsReducerObj = new CachedDataReducer(
  clusterUiApi.getTxnInsightEvents,
  "transactionInsights",
  null,
  moment.duration(5, "m"),
);
export const refreshTxnContentionInsights =
  transactionInsightsReducerObj.refresh;

export const refreshTransactionInsights = (
  req?: clusterUiApi.ExecutionInsightsRequest,
): ThunkAction<any, any, any, Action> => {
  return (dispatch: ThunkDispatch<unknown, unknown, Action>) => {
    dispatch(refreshTxnContentionInsights(req));
    dispatch(refreshExecutionInsights(req));
  };
};
export const invalidateTransactionInsights =
  transactionInsightsReducerObj.invalidateData;

const executionInsightsReducerObj = new CachedDataReducer(
  clusterUiApi.getClusterInsightsApi,
  "executionInsights",
  null,
  moment.duration(5, "m"),
);
export const refreshExecutionInsights = executionInsightsReducerObj.refresh;
export const invalidateExecutionInsights =
  executionInsightsReducerObj.invalidateData;

export const transactionInsightRequestKey = (
  req: clusterUiApi.TxnContentionInsightDetailsRequest,
): string => `${req.id}`;

const transactionInsightDetailsReducerObj = new KeyedCachedDataReducer(
  clusterUiApi.getTransactionInsightEventDetailsState,
  "transactionInsightDetails",
  transactionInsightRequestKey,
  null,
  moment.duration(5, "m"),
);

const refreshTxnContentionInsightDetails =
  transactionInsightDetailsReducerObj.refresh;

export const refreshTransactionInsightDetails = (
  req: clusterUiApi.TxnContentionInsightDetailsRequest,
): ThunkAction<any, any, any, Action> => {
  return (dispatch: ThunkDispatch<unknown, unknown, Action>) => {
    dispatch(refreshTxnContentionInsightDetails(req));
    dispatch(refreshExecutionInsights({ start: req.start, end: req.end }));
  };
};

const schemaInsightsReducerObj = new CachedDataReducer(
  clusterUiApi.getSchemaInsights,
  "schemaInsights",
  null,
  moment.duration(5, "m"),
);
export const refreshSchemaInsights = schemaInsightsReducerObj.refresh;

export const schedulesKey = (req: { status: string; limit: number }) =>
  `${encodeURIComponent(req.status)}/${encodeURIComponent(
    req.limit?.toString(),
  )}`;

const schedulesReducerObj = new KeyedCachedDataReducer(
  clusterUiApi.getSchedules,
  "schedules",
  schedulesKey,
  moment.duration(10, "s"),
  moment.duration(1, "minute"),
);
export const refreshSchedules = schedulesReducerObj.refresh;

export const scheduleKey = (scheduleID: Long): string => scheduleID.toString();

const scheduleReducerObj = new KeyedCachedDataReducer(
  clusterUiApi.getSchedule,
  "schedule",
  scheduleKey,
  moment.duration(10, "s"),
);
export const refreshSchedule = scheduleReducerObj.refresh;

const snapshotsReducerObj = new KeyedCachedDataReducer(
  clusterUiApi.listTracingSnapshots,
  "snapshots",
  (nodeID: string): string => nodeID,
);
export const refreshSnapshots = snapshotsReducerObj.refresh;

export const snapshotKey = (req: {
  nodeID: string;
  snapshotID: number;
}): string => req.nodeID + "/" + req.snapshotID.toString();

const snapshotReducerObj = new KeyedCachedDataReducer(
  clusterUiApi.getTracingSnapshot,
  "snapshot",
  snapshotKey,
);
export const refreshSnapshot = snapshotReducerObj.refresh;

export const rawTraceKey = (req: {
  nodeID: string;
  snapshotID: number;
  traceID: Long;
}): string =>
  req.nodeID + "/" + req.snapshotID.toString() + "/" + req.traceID?.toString();
const rawTraceReducerObj = new KeyedCachedDataReducer(
  clusterUiApi.getRawTrace,
  "rawTrace",
  rawTraceKey,
);
export const refreshRawTrace = rawTraceReducerObj.refresh;

export interface APIReducersState {
  cluster: CachedDataReducerState<api.ClusterResponseMessage>;
  events: CachedDataReducerState<clusterUiApi.EventsResponse>;
  health: HealthState;
  nodes: CachedDataReducerState<INodeStatus[]>;
  raft: CachedDataReducerState<api.RaftDebugResponseMessage>;
  version: CachedDataReducerState<VersionList>;
  locations: CachedDataReducerState<api.LocationsResponseMessage>;
  databases: CachedDataReducerState<clusterUiApi.DatabasesListResponse>;
  databaseDetails: KeyedCachedDataReducerState<api.DatabaseDetailsResponseMessage>;
  tableDetails: KeyedCachedDataReducerState<api.TableDetailsResponseMessage>;
  tableStats: KeyedCachedDataReducerState<api.TableStatsResponseMessage>;
  indexStats: KeyedCachedDataReducerState<api.IndexStatsResponseMessage>;
  nonTableStats: CachedDataReducerState<api.NonTableStatsResponseMessage>;
  logs: CachedDataReducerState<api.LogEntriesResponseMessage>;
  liveness: CachedDataReducerState<api.LivenessResponseMessage>;
  jobs: KeyedCachedDataReducerState<api.JobsResponseMessage>;
  job: KeyedCachedDataReducerState<api.JobResponseMessage>;
  queryPlan: CachedDataReducerState<api.QueryPlanResponseMessage>;
  problemRanges: KeyedCachedDataReducerState<api.ProblemRangesResponseMessage>;
  certificates: KeyedCachedDataReducerState<api.CertificatesResponseMessage>;
  range: KeyedCachedDataReducerState<api.RangeResponseMessage>;
  allocatorRange: KeyedCachedDataReducerState<api.AllocatorRangeResponseMessage>;
  rangeLog: KeyedCachedDataReducerState<api.RangeLogResponseMessage>;
  sessions: CachedDataReducerState<api.SessionsResponseMessage>;
  settings: CachedDataReducerState<api.SettingsResponseMessage>;
  stores: KeyedCachedDataReducerState<api.StoresResponseMessage>;
  statements: CachedDataReducerState<api.StatementsResponseMessage>;
  statementDetails: KeyedCachedDataReducerState<api.StatementDetailsResponseMessage>;
  dataDistribution: CachedDataReducerState<api.DataDistributionResponseMessage>;
  metricMetadata: CachedDataReducerState<api.MetricMetadataResponseMessage>;
  statementDiagnosticsReports: CachedDataReducerState<clusterUiApi.StatementDiagnosticsResponse>;
  userSQLRoles: CachedDataReducerState<api.UserSQLRolesResponseMessage>;
  hotRanges: PaginatedCachedDataReducerState<api.HotRangesV2ResponseMessage>;
  clusterLocks: CachedDataReducerState<clusterUiApi.ClusterLocksResponse>;
  transactionInsights: CachedDataReducerState<TxnContentionInsightEvent[]>;
  transactionInsightDetails: KeyedCachedDataReducerState<TxnContentionInsightDetails>;
  executionInsights: CachedDataReducerState<TxnInsightEvent[]>;
  schemaInsights: CachedDataReducerState<clusterUiApi.InsightRecommendation[]>;
  schedules: KeyedCachedDataReducerState<clusterUiApi.Schedules>;
  schedule: KeyedCachedDataReducerState<clusterUiApi.Schedule>;
  snapshots: KeyedCachedDataReducerState<clusterUiApi.ListTracingSnapshotsResponse>;
  snapshot: KeyedCachedDataReducerState<clusterUiApi.GetTracingSnapshotResponse>;
  rawTrace: KeyedCachedDataReducerState<clusterUiApi.GetTraceResponse>;
}

export const apiReducersReducer = combineReducers<APIReducersState>({
  [clusterReducerObj.actionNamespace]: clusterReducerObj.reducer,
  [eventsReducerObj.actionNamespace]: eventsReducerObj.reducer,
  [healthReducerObj.actionNamespace]: healthReducerObj.reducer,
  [nodesReducerObj.actionNamespace]: nodesReducerObj.reducer,
  [raftReducerObj.actionNamespace]: raftReducerObj.reducer,
  [versionReducerObj.actionNamespace]: versionReducerObj.reducer,
  [locationsReducerObj.actionNamespace]: locationsReducerObj.reducer,
  [databasesReducerObj.actionNamespace]: databasesReducerObj.reducer,
  [databaseDetailsReducerObj.actionNamespace]:
    databaseDetailsReducerObj.reducer,
  [tableDetailsReducerObj.actionNamespace]: tableDetailsReducerObj.reducer,
  [tableStatsReducerObj.actionNamespace]: tableStatsReducerObj.reducer,
  [indexStatsReducerObj.actionNamespace]: indexStatsReducerObj.reducer,
  [nonTableStatsReducerObj.actionNamespace]: nonTableStatsReducerObj.reducer,
  [logsReducerObj.actionNamespace]: logsReducerObj.reducer,
  [livenessReducerObj.actionNamespace]: livenessReducerObj.reducer,
  [jobsReducerObj.actionNamespace]: jobsReducerObj.reducer,
  [jobReducerObj.actionNamespace]: jobReducerObj.reducer,
  [queryPlanReducerObj.actionNamespace]: queryPlanReducerObj.reducer,
  [problemRangesReducerObj.actionNamespace]: problemRangesReducerObj.reducer,
  [certificatesReducerObj.actionNamespace]: certificatesReducerObj.reducer,
  [rangeReducerObj.actionNamespace]: rangeReducerObj.reducer,
  [allocatorRangeReducerObj.actionNamespace]: allocatorRangeReducerObj.reducer,
  [rangeLogReducerObj.actionNamespace]: rangeLogReducerObj.reducer,
  [settingsReducerObj.actionNamespace]: settingsReducerObj.reducer,
  [sessionsReducerObj.actionNamespace]: sessionsReducerObj.reducer,
  [storesReducerObj.actionNamespace]: storesReducerObj.reducer,
  [queriesReducerObj.actionNamespace]: queriesReducerObj.reducer,
  [statementDetailsReducerObj.actionNamespace]:
    statementDetailsReducerObj.reducer,
  [dataDistributionReducerObj.actionNamespace]:
    dataDistributionReducerObj.reducer,
  [metricMetadataReducerObj.actionNamespace]: metricMetadataReducerObj.reducer,
  [statementDiagnosticsReportsReducerObj.actionNamespace]:
    statementDiagnosticsReportsReducerObj.reducer,
  [userSQLRolesReducerObj.actionNamespace]: userSQLRolesReducerObj.reducer,
  [hotRangesReducerObj.actionNamespace]: hotRangesReducerObj.reducer,
  [clusterLocksReducerObj.actionNamespace]: clusterLocksReducerObj.reducer,
  [transactionInsightsReducerObj.actionNamespace]:
    transactionInsightsReducerObj.reducer,
  [transactionInsightDetailsReducerObj.actionNamespace]:
    transactionInsightDetailsReducerObj.reducer,
  [executionInsightsReducerObj.actionNamespace]:
    executionInsightsReducerObj.reducer,
  [schemaInsightsReducerObj.actionNamespace]: schemaInsightsReducerObj.reducer,
  [schedulesReducerObj.actionNamespace]: schedulesReducerObj.reducer,
  [scheduleReducerObj.actionNamespace]: scheduleReducerObj.reducer,
  [snapshotsReducerObj.actionNamespace]: snapshotsReducerObj.reducer,
  [snapshotReducerObj.actionNamespace]: snapshotReducerObj.reducer,
  [rawTraceReducerObj.actionNamespace]: rawTraceReducerObj.reducer,
});

export { CachedDataReducerState, KeyedCachedDataReducerState };
