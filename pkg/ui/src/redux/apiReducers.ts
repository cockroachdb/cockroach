import _ from "lodash";
import { combineReducers } from "redux";
import moment from "moment";

import { CachedDataReducer, CachedDataReducerState, KeyedCachedDataReducer, KeyedCachedDataReducerState } from "./cachedDataReducer";
import * as api from "../util/api";
import { VersionList } from "../interfaces/cockroachlabs";
import { versionCheck } from "../util/cockroachlabsAPI";
import { NodeStatus$Properties, RollupStoreMetrics } from "../util/proto";

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

const raftReducerObj = new CachedDataReducer(api.raftDebug, "raft");
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
});

export {CachedDataReducerState, KeyedCachedDataReducerState};
