import _ from "lodash";
import { combineReducers } from "redux";
import moment from "moment";

import { CachedDataReducer, CachedDataReducerState, KeyedCachedDataReducer, KeyedCachedDataReducerState } from "./cachedDataReducer";
import * as api from "src/util/api";
import { VersionList } from "src/interfaces/cockroachlabs";
import { versionCheck } from "src/util/cockroachlabsAPI";
import { NodeStatus$Properties, RollupStoreMetrics } from "src/util/proto";
import * as protos from "src/js/protos";

// The primary export of this file are the "refresh" functions of the various
// reducers, which are used by many react components to request fresh data.
// However, some of the reducer objects are also fully exported for use
// in tests.

export const clusterReducerObj = new CachedDataReducer(api.getCluster, "cluster");
export const refreshCluster = clusterReducerObj.refresh;

const eventsReducerObj = new CachedDataReducer(api.getEvents, "events", moment.duration(10, "s"));
export const refreshEvents = eventsReducerObj.refresh;

export type HealthState = CachedDataReducerState<api.HealthResponseMessage>;
export const healthReducerObj = new CachedDataReducer(api.getHealth, "health", moment.duration(2, "s"));
export const refreshHealth = healthReducerObj.refresh;

function rollupStoreMetrics(res: api.NodesResponseMessage): NodeStatus$Properties[] {
  return _.map(res.nodes, (node) => {
    RollupStoreMetrics(node);
    return node;
  });
}

export const nodesReducerObj = new CachedDataReducer((req: api.NodesRequestMessage, timeout?: moment.Duration) => api.getNodes(req, timeout).then(rollupStoreMetrics), "nodes", moment.duration(10, "s"));
export const refreshNodes = nodesReducerObj.refresh;

const raftReducerObj = new CachedDataReducer(api.raftDebug, "raft", moment.duration(10, "s"));
export const refreshRaft = raftReducerObj.refresh;

export const versionReducerObj = new CachedDataReducer(versionCheck, "version");
export const refreshVersion = versionReducerObj.refresh;

const databasesReducerObj = new CachedDataReducer(api.getDatabaseList, "databases");
export const refreshDatabases = databasesReducerObj.refresh;

export const databaseRequestToID = (req: api.DatabaseDetailsRequestMessage): string => req.database;

const databaseDetailsReducerObj = new KeyedCachedDataReducer(api.getDatabaseDetails, "databaseDetails", databaseRequestToID);
export const refreshDatabaseDetails = databaseDetailsReducerObj.refresh;

// NOTE: We encode the db and table name so we can combine them as a string.
// TODO(maxlang): there's probably a nicer way to do this
export function generateTableID(db: string, table: string) {
  return `${encodeURIComponent(db)}/${encodeURIComponent(table)}`;
}

export const tableRequestToID = (req: api.TableDetailsRequestMessage | api.TableStatsRequestMessage): string => generateTableID(req.database, req.table);

const tableDetailsReducerObj = new KeyedCachedDataReducer(api.getTableDetails, "tableDetails", tableRequestToID);
export const refreshTableDetails = tableDetailsReducerObj.refresh;

const tableStatsReducerObj = new KeyedCachedDataReducer(api.getTableStats, "tableStats", tableRequestToID);
export const refreshTableStats = tableStatsReducerObj.refresh;

const logsReducerObj = new CachedDataReducer(api.getLogs, "logs", moment.duration(10, "s"));
export const refreshLogs = logsReducerObj.refresh;

const livenessReducerObj = new CachedDataReducer(api.getLiveness, "liveness", moment.duration(10, "s"));
export const refreshLiveness = livenessReducerObj.refresh;

export const jobsKey = (status: string, type: protos.cockroach.sql.jobs.Type, limit: number) =>
  `${encodeURIComponent(status)}/${encodeURIComponent(type.toString())}/${encodeURIComponent(limit.toString())}`;

const jobsRequestKey = (req: api.JobsRequestMessage): string =>
  jobsKey(req.status, req.type, req.limit);

const jobsReducerObj = new KeyedCachedDataReducer(api.getJobs, "jobs", jobsRequestKey, moment.duration(10, "s"));
export const refreshJobs = jobsReducerObj.refresh;

export const queryToID = (req: api.QueryPlanRequestMessage): string => req.query;

const queryPlanReducerObj = new CachedDataReducer(api.getQueryPlan, "queryPlan");
export const refreshQueryPlan = queryPlanReducerObj.refresh;

const problemRangesReducerObj = new CachedDataReducer(api.getProblemRanges, "problemRanges", moment.duration(10, "s"));
export const refreshProblemRanges = problemRangesReducerObj.refresh;

const certificatesReducerObj = new CachedDataReducer(api.getCertificates, "certificates", moment.duration(10, "s"));
export const refreshCertificates = certificatesReducerObj.refresh;

const rangeReducerObj = new CachedDataReducer(api.getRange, "range", moment.duration(10, "s"));
export const refreshRange = rangeReducerObj.refresh;

const allocatorRangeReducerObj = new CachedDataReducer(api.getAllocatorRange, "allocatorRange", moment.duration(10, "s"));
export const refreshAllocatorRange = allocatorRangeReducerObj.refresh;

const rangeLogReducerObj = new CachedDataReducer(api.getRangeLog, "rangeLog", moment.duration(10, "m"));
export const refreshRangeLog = rangeLogReducerObj.refresh;

export interface APIReducersState {
  cluster: CachedDataReducerState<api.ClusterResponseMessage>;
  events: CachedDataReducerState<api.EventsResponseMessage>;
  health: HealthState;
  nodes: CachedDataReducerState<NodeStatus$Properties[]>;
  raft: CachedDataReducerState<api.RaftDebugResponseMessage>;
  version: CachedDataReducerState<VersionList>;
  databases: CachedDataReducerState<api.DatabasesResponseMessage>;
  databaseDetails: KeyedCachedDataReducerState<api.DatabaseDetailsResponseMessage>;
  tableDetails: KeyedCachedDataReducerState<api.TableDetailsResponseMessage>;
  tableStats: KeyedCachedDataReducerState<api.TableStatsResponseMessage>;
  logs: CachedDataReducerState<api.LogEntriesResponseMessage>;
  liveness: CachedDataReducerState<api.LivenessResponseMessage>;
  jobs: KeyedCachedDataReducerState<api.JobsResponseMessage>;
  queryPlan: CachedDataReducerState<api.QueryPlanResponseMessage>;
  problemRanges: CachedDataReducerState<api.ProblemRangesResponseMessage>;
  certificates: CachedDataReducerState<api.CertificatesResponseMessage>;
  range: CachedDataReducerState<api.RangeResponseMessage>;
  allocatorRange: CachedDataReducerState<api.AllocatorRangeResponseMessage>;
  rangeLog: CachedDataReducerState<api.RangeLogResponseMessage>;
}

export default combineReducers<APIReducersState>({
  [clusterReducerObj.actionNamespace]: clusterReducerObj.reducer,
  [eventsReducerObj.actionNamespace]: eventsReducerObj.reducer,
  [healthReducerObj.actionNamespace]: healthReducerObj.reducer,
  [nodesReducerObj.actionNamespace]: nodesReducerObj.reducer,
  [raftReducerObj.actionNamespace]: raftReducerObj.reducer,
  [versionReducerObj.actionNamespace]: versionReducerObj.reducer,
  [databasesReducerObj.actionNamespace]: databasesReducerObj.reducer,
  [databaseDetailsReducerObj.actionNamespace]: databaseDetailsReducerObj.reducer,
  [tableDetailsReducerObj.actionNamespace]: tableDetailsReducerObj.reducer,
  [tableStatsReducerObj.actionNamespace]: tableStatsReducerObj.reducer,
  [logsReducerObj.actionNamespace]: logsReducerObj.reducer,
  [livenessReducerObj.actionNamespace]: livenessReducerObj.reducer,
  [jobsReducerObj.actionNamespace]: jobsReducerObj.reducer,
  [queryPlanReducerObj.actionNamespace]: queryPlanReducerObj.reducer,
  [problemRangesReducerObj.actionNamespace]: problemRangesReducerObj.reducer,
  [certificatesReducerObj.actionNamespace]: certificatesReducerObj.reducer,
  [rangeReducerObj.actionNamespace]: rangeReducerObj.reducer,
  [allocatorRangeReducerObj.actionNamespace]: allocatorRangeReducerObj.reducer,
  [rangeLogReducerObj.actionNamespace]: rangeLogReducerObj.reducer,
});

export {CachedDataReducerState, KeyedCachedDataReducerState};
