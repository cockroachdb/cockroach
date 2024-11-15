// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import {
  api as clusterUiApi,
  util,
  StmtInsightEvent,
  TxnInsightEvent,
} from "@cockroachlabs/cluster-ui";
import isEmpty from "lodash/isEmpty";
import isNil from "lodash/isNil";
import map from "lodash/map";
import sortby from "lodash/sortBy";
import Long from "long";
import moment from "moment-timezone";
import { RouteComponentProps } from "react-router";
import { Action, combineReducers } from "redux";
import { ThunkAction, ThunkDispatch } from "redux-thunk";
import { createSelector, ParametricSelector } from "reselect";

import { VersionList } from "src/interfaces/cockroachlabs";
import * as protos from "src/js/protos";
import * as api from "src/util/api";
import { versionCheck } from "src/util/cockroachlabsAPI";
import { INodeStatus, RollupStoreMetrics } from "src/util/proto";

import {
  CachedDataReducer,
  CachedDataReducerState,
  KeyedCachedDataReducer,
  KeyedCachedDataReducerState,
  PaginatedCachedDataReducer,
  PaginatedCachedDataReducerState,
} from "./cachedDataReducer";
import { AdminUIState } from "./state";

const { generateStmtDetailsToID, HexStringToInt64String, generateTableID } =
  util;

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
  return map(res.nodes, node => {
    RollupStoreMetrics(node);
    return node;
  });
}

export const nodesReducerObj = new CachedDataReducer(
  (req: api.NodesRequestMessage, timeout?: moment.Duration) =>
    api
      .getNodesUI(req, timeout)
      .then(rollupStoreMetrics)
      .then(nodeStatuses => {
        nodeStatuses.forEach(ns => {
          ns.store_statuses = sortby(ns.store_statuses, ss => ss.desc.store_id);
        });
        return nodeStatuses;
      }),
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

const hotRangesRequestToID = (req: api.HotRangesRequestMessage) =>
  req.page_token;

export const hotRangesReducerObj = new PaginatedCachedDataReducer(
  api.getHotRanges,
  "hotRanges",
  hotRangesRequestToID,
  1000 /* page limit */,
  null /* invalidation period */,
  moment.duration(30, "minutes"),
);

export const refreshHotRanges = hotRangesReducerObj.refresh;

export const tableRequestToID = (req: api.IndexStatsRequestMessage): string =>
  generateTableID(req.database, req.table);

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

export const jobProfilerRequestKey = (
  req: api.ListJobProfilerExecutionDetailsRequestMessage,
): string => `${req.job_id}`;

const jobProfilerReducerObj = new KeyedCachedDataReducer(
  api.listExecutionDetailFiles,
  "jobProfiler",
  jobProfilerRequestKey,
  null,
  moment.duration(10, "m"),
);
export const refreshListExecutionDetailFiles = jobProfilerReducerObj.refresh;

export const queryToID = (req: api.QueryPlanRequestMessage): string =>
  req.query;

const queryPlanReducerObj = new CachedDataReducer(
  api.getQueryPlan,
  "queryPlan",
);
export const refreshQueryPlan = queryPlanReducerObj.refresh;

export const problemRangesRequestKey = (
  req: api.ProblemRangesRequestMessage,
): string => (isEmpty(req.node_id) ? "all" : req.node_id);

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
): string => (isEmpty(req.node_id) ? "none" : req.node_id);

const certificatesReducerObj = new KeyedCachedDataReducer(
  api.getCertificates,
  "certificates",
  certificatesRequestKey,
  moment.duration(1, "m"),
);
export const refreshCertificates = certificatesReducerObj.refresh;

export const rangeRequestKey = (req: api.RangeRequestMessage): string =>
  isNil(req.range_id) ? "none" : req.range_id.toString();

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
): string => (isNil(req.range_id) ? "none" : req.range_id.toString());

const allocatorRangeReducerObj = new KeyedCachedDataReducer(
  api.getAllocatorRange,
  "allocatorRange",
  allocatorRangeRequestKey,
  moment.duration(0),
  moment.duration(1, "m"),
);
export const refreshAllocatorRange = allocatorRangeReducerObj.refresh;

export const rangeLogRequestKey = (req: api.RangeLogRequestMessage): string =>
  isNil(req.range_id) ? "none" : req.range_id.toString();

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
  isEmpty(req.node_id) ? "none" : req.node_id;

const storesReducerObj = new KeyedCachedDataReducer(
  api.getStores,
  "stores",
  storesRequestKey,
  moment.duration(0),
  moment.duration(1, "m"),
);
export const refreshStores = storesReducerObj.refresh;

const queriesReducerObj = new CachedDataReducer(
  clusterUiApi.getCombinedStatements,
  "statements",
  null,
  moment.duration(10, "m"),
  true,
);
export const invalidateStatements = queriesReducerObj.invalidateData;
export const refreshStatements = queriesReducerObj.refresh;

const txnFingerprintStatsReducerObj = new CachedDataReducer(
  clusterUiApi.getFlushedTxnStatsApi,
  "transactions",
  null,
  moment.duration(30, "m"),
  true,
);
export const invalidateTxns = txnFingerprintStatsReducerObj.invalidateData;
export const refreshTxns = txnFingerprintStatsReducerObj.refresh;

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
  null,
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

export const statementDiagnosticInvalidationPeriod = moment.duration(5, "m");
const statementDiagnosticsReportsReducerObj = new CachedDataReducer(
  clusterUiApi.getStatementDiagnosticsReports,
  "statementDiagnosticsReports",
  statementDiagnosticInvalidationPeriod,
  moment.duration(1, "m"),
);
export const refreshStatementDiagnosticsRequests =
  statementDiagnosticsReportsReducerObj.refresh;
export const invalidateStatementDiagnosticsRequests =
  statementDiagnosticsReportsReducerObj.invalidateData;
export const RECEIVE_STATEMENT_DIAGNOSTICS_REPORT =
  statementDiagnosticsReportsReducerObj.RECEIVE;

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

const stmtInsightsReducerObj = new CachedDataReducer(
  clusterUiApi.getStmtInsightsApi,
  "stmtInsights",
  null,
  moment.duration(5, "m"),
);
export const refreshStmtInsights = stmtInsightsReducerObj.refresh;
export const invalidateExecutionInsights =
  stmtInsightsReducerObj.invalidateData;

const txnInsightsReducerObj = new CachedDataReducer(
  clusterUiApi.getTxnInsightsApi,
  "txnInsights",
  null,
  moment.duration(5, "m"),
);
export const refreshTxnInsights = txnInsightsReducerObj.refresh;
export const invalidateTxnInsights = txnInsightsReducerObj.invalidateData;

export const txnInsightsRequestKey = (
  req: clusterUiApi.TxnInsightDetailsRequest,
): string => req.txnExecutionID;

const txnInsightDetailsReducerObj = new KeyedCachedDataReducer(
  clusterUiApi.getTxnInsightDetailsApi,
  "txnInsightDetails",
  txnInsightsRequestKey,
  null,
  moment.duration(5, "m"),
);

export const refreshTxnInsightDetails = txnInsightDetailsReducerObj.refresh;

export const statementFingerprintInsightRequestKey = (
  req: clusterUiApi.StmtInsightsReq,
): string => `${HexStringToInt64String(req.stmtFingerprintId)}`;

const statementFingerprintInsightsReducerObj = new KeyedCachedDataReducer(
  clusterUiApi.getStmtInsightsApi,
  "statementFingerprintInsights",
  statementFingerprintInsightRequestKey,
  null,
  moment.duration(5, "m"),
);

export const refreshStatementFingerprintInsights =
  statementFingerprintInsightsReducerObj.refresh;

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

const tenantsListObj = new CachedDataReducer(
  api.getTenants,
  "tenants",
  moment.duration(60, "m"),
);

export const refreshTenantsList = tenantsListObj.refresh;

const connectivityObj = new CachedDataReducer(
  api.getNetworkConnectivity,
  "connectivity",
  moment.duration(30, "s"),
  moment.duration(1, "minute"),
);

export const refreshConnectivity = connectivityObj.refresh;

export interface APIReducersState {
  cluster: CachedDataReducerState<api.ClusterResponseMessage>;
  events: CachedDataReducerState<
    clusterUiApi.SqlApiResponse<clusterUiApi.EventsResponse>
  >;
  health: HealthState;
  nodes: CachedDataReducerState<INodeStatus[]>;
  raft: CachedDataReducerState<api.RaftDebugResponseMessage>;
  version: CachedDataReducerState<VersionList>;
  locations: CachedDataReducerState<api.LocationsResponseMessage>;
  databases: CachedDataReducerState<clusterUiApi.DatabasesListResponse>;
  indexStats: KeyedCachedDataReducerState<api.IndexStatsResponseMessage>;
  nonTableStats: CachedDataReducerState<api.NonTableStatsResponseMessage>;
  logs: CachedDataReducerState<api.LogEntriesResponseMessage>;
  liveness: CachedDataReducerState<api.LivenessResponseMessage>;
  jobProfiler: KeyedCachedDataReducerState<api.ListJobProfilerExecutionDetailsResponseMessage>;
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
  statements: CachedDataReducerState<clusterUiApi.SqlStatsResponse>;
  transactions: CachedDataReducerState<clusterUiApi.SqlStatsResponse>;
  statementDetails: KeyedCachedDataReducerState<api.StatementDetailsResponseMessage>;
  dataDistribution: CachedDataReducerState<api.DataDistributionResponseMessage>;
  metricMetadata: CachedDataReducerState<api.MetricMetadataResponseMessage>;
  statementDiagnosticsReports: CachedDataReducerState<clusterUiApi.StatementDiagnosticsResponse>;
  userSQLRoles: CachedDataReducerState<api.UserSQLRolesResponseMessage>;
  hotRanges: PaginatedCachedDataReducerState<api.HotRangesV2ResponseMessage>;
  clusterLocks: CachedDataReducerState<
    clusterUiApi.SqlApiResponse<clusterUiApi.ClusterLocksResponse>
  >;
  stmtInsights: CachedDataReducerState<
    clusterUiApi.SqlApiResponse<StmtInsightEvent[]>
  >;
  txnInsightDetails: KeyedCachedDataReducerState<
    clusterUiApi.SqlApiResponse<clusterUiApi.TxnInsightDetailsResponse>
  >;
  txnInsights: CachedDataReducerState<
    clusterUiApi.SqlApiResponse<TxnInsightEvent[]>
  >;
  schemaInsights: CachedDataReducerState<
    clusterUiApi.SqlApiResponse<clusterUiApi.InsightRecommendation[]>
  >;
  statementFingerprintInsights: KeyedCachedDataReducerState<
    clusterUiApi.SqlApiResponse<StmtInsightEvent[]>
  >;
  schedules: KeyedCachedDataReducerState<clusterUiApi.Schedules>;
  schedule: KeyedCachedDataReducerState<clusterUiApi.Schedule>;
  snapshots: KeyedCachedDataReducerState<clusterUiApi.ListTracingSnapshotsResponse>;
  snapshot: KeyedCachedDataReducerState<clusterUiApi.GetTracingSnapshotResponse>;
  rawTrace: KeyedCachedDataReducerState<clusterUiApi.GetTraceResponse>;
  tenants: CachedDataReducerState<api.ListTenantsResponseMessage>;
  connectivity: CachedDataReducerState<api.NetworkConnectivityResponse>;
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
  [indexStatsReducerObj.actionNamespace]: indexStatsReducerObj.reducer,
  [nonTableStatsReducerObj.actionNamespace]: nonTableStatsReducerObj.reducer,
  [logsReducerObj.actionNamespace]: logsReducerObj.reducer,
  [livenessReducerObj.actionNamespace]: livenessReducerObj.reducer,
  [jobProfilerReducerObj.actionNamespace]: jobProfilerReducerObj.reducer,
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
  [txnFingerprintStatsReducerObj.actionNamespace]:
    txnFingerprintStatsReducerObj.reducer,
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
  [txnInsightsReducerObj.actionNamespace]: txnInsightsReducerObj.reducer,
  [txnInsightDetailsReducerObj.actionNamespace]:
    txnInsightDetailsReducerObj.reducer,
  [stmtInsightsReducerObj.actionNamespace]: stmtInsightsReducerObj.reducer,
  [schemaInsightsReducerObj.actionNamespace]: schemaInsightsReducerObj.reducer,
  [schedulesReducerObj.actionNamespace]: schedulesReducerObj.reducer,
  [scheduleReducerObj.actionNamespace]: scheduleReducerObj.reducer,
  [snapshotsReducerObj.actionNamespace]: snapshotsReducerObj.reducer,
  [snapshotReducerObj.actionNamespace]: snapshotReducerObj.reducer,
  [rawTraceReducerObj.actionNamespace]: rawTraceReducerObj.reducer,
  [statementFingerprintInsightsReducerObj.actionNamespace]:
    statementFingerprintInsightsReducerObj.reducer,
  [tenantsListObj.actionNamespace]: tenantsListObj.reducer,
  [connectivityObj.actionNamespace]: connectivityObj.reducer,
});

export { CachedDataReducerState, KeyedCachedDataReducerState };

// This mapped type assigns keys in object type T where the key value
// is of type V to the key value. Otherwise, it assigns it 'never'.
// This enables one to extract keys in an object T of type V.
// Example:
// type MyObject = {
//   a: string;
//   b: string;
// . c: number;
// }
// type KeysThatHaveStringsInMyObject = KeysMatching<MyObject, string>;
// KeysThatHaveStringsInMyObject maps to the following type:
// Result = {
//    a: 'a';
// .  b: 'b';
// .  c: never;
// }
//
type KeysMatchingType<T, V> = {
  [K in keyof T]: T[K] extends V ? K : never;
}[keyof T];

type CachedDataTypesInState = {
  [K in keyof APIReducersState]: APIReducersState[K] extends CachedDataReducerState<unknown>
    ? APIReducersState[K]["data"]
    : never;
}[keyof APIReducersState];

// These selector creators are used to create selectors that will map the
// cached data reducer state to the expected  'clusterUiApi.RequestState'
// type. It also prevents passing a new object when the underlying cached
// data reducer hasn't changed.
export function createSelectorForCachedDataField<
  RespType extends CachedDataTypesInState,
>(
  fieldName: KeysMatchingType<
    APIReducersState,
    CachedDataReducerState<RespType>
  >,
) {
  return createSelector<
    AdminUIState,
    CachedDataReducerState<RespType>,
    clusterUiApi.RequestState<RespType>
  >(
    (state: AdminUIState): CachedDataReducerState<RespType> =>
      state.cachedData[fieldName] as CachedDataReducerState<RespType>,
    (response): clusterUiApi.RequestState<RespType> => {
      return {
        data: response?.data,
        error: response?.lastError,
        valid: response?.valid ?? false,
        inFlight: response?.inFlight ?? false,
        lastUpdated: response?.setAt,
      };
    },
  );
}

// Extract the data types we store in the KeyedCachedData manager.
type KeyedCachedDataTypesInState = {
  [K in keyof APIReducersState]: APIReducersState[K] extends KeyedCachedDataReducerState<unknown>
    ? APIReducersState[K][string]["data"]
    : never;
}[keyof APIReducersState];

export function createSelectorForKeyedCachedDataField<
  RespType extends KeyedCachedDataTypesInState,
>(
  fieldName: KeysMatchingType<
    APIReducersState,
    KeyedCachedDataReducerState<RespType>
  >,
  selectKey: ParametricSelector<AdminUIState, RouteComponentProps, string>,
) {
  return createSelector<
    AdminUIState,
    RouteComponentProps,
    KeyedCachedDataReducerState<RespType>,
    string,
    clusterUiApi.RequestState<RespType>
  >(
    (state: AdminUIState, _props: RouteComponentProps) =>
      state.cachedData[fieldName] as KeyedCachedDataReducerState<RespType>,
    selectKey,
    (response, key): clusterUiApi.RequestState<RespType> => {
      const cachedEntry = response[key];

      return {
        data: cachedEntry?.data,
        error: cachedEntry?.lastError,
        valid: cachedEntry?.valid ?? true,
        inFlight: cachedEntry?.inFlight ?? false,
        lastUpdated: cachedEntry?.setAt,
      };
    },
  );
}
