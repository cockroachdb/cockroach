import _ = require("lodash");
import { combineReducers } from "redux";
import { CachedDataReducer, CachedDataReducerState, KeyedCachedDataReducer, KeyedCachedDataReducerState } from "./cachedDataReducer";
import * as api from "../util/api";
import { VersionList } from "../interfaces/cockroachlabs";
import { versionCheck } from "../util/cockroachlabsAPI";
import { NodeStatus, RollupStoreMetrics } from "../util/proto";

const TEN_SECONDS = 10 * 1000;
const TWO_SECONDS = 2 * 1000;

const clusterReducerObj = new CachedDataReducer(api.getCluster, "cluster");
export const refreshCluster = clusterReducerObj.refresh;

const eventsReducerObj = new CachedDataReducer(api.getEvents, "events", TEN_SECONDS);
export const refreshEvents = eventsReducerObj.refresh;

export type HealthState = CachedDataReducerState<api.HealthResponseMessage>;
const healthReducerObj = new CachedDataReducer(api.getHealth, "health", TWO_SECONDS);
export const refreshHealth = healthReducerObj.refresh;

function rollupStoreMetrics(res: api.NodesResponseMessage): NodeStatus[] {
  return _.map(res.nodes, (node) => {
    RollupStoreMetrics(node);
    return node;
  });
}

const nodesReducerObj = new CachedDataReducer(() => api.getNodes().then(rollupStoreMetrics), "nodes", TEN_SECONDS);
export const refreshNodes = nodesReducerObj.refresh;

const raftReducerObj = new CachedDataReducer(api.raftDebug, "raft");
export const refreshRaft = raftReducerObj.refresh;

const versionReducerObj = new CachedDataReducer(versionCheck, "version");
export const refreshVersion = versionReducerObj.refresh;

const databasesReducerObj = new CachedDataReducer(api.getDatabaseList, "databases");
export const refreshDatabases = databasesReducerObj.refresh;

export const databaseRequestToID = (req: api.DatabaseDetailsRequest): string => req.database;

const databaseDetailsReducerObj = new KeyedCachedDataReducer(api.getDatabaseDetails, "databaseDetails", databaseRequestToID);
export const refreshDatabaseDetails = databaseDetailsReducerObj.refresh;

// NOTE: We encode the db and table name so we can combine them as a string.
// TODO(maxlang): there's probably a nicer way to do this
export function generateTableID(db: string, table: string) {
  return `${encodeURIComponent(db)}/${encodeURIComponent(table)}`;
}

export const tableRequestToID = (req: api.TableDetailsRequest| api.TableStatsRequest): string => generateTableID(req.database, req.table);

const tableDetailsReducerObj = new KeyedCachedDataReducer(api.getTableDetails, "tableDetails", tableRequestToID);
export const refreshTableDetails = tableDetailsReducerObj.refresh;

const tableStatsReducerObj = new KeyedCachedDataReducer(api.getTableStats, "tableStats", tableRequestToID);
export const refreshTableStats = tableStatsReducerObj.refresh;

export interface APIReducersState {
  cluster: CachedDataReducerState<api.ClusterResponseMessage>;
  events: CachedDataReducerState<api.EventsResponseMessage>;
  health: HealthState;
  nodes: CachedDataReducerState<NodeStatus[]>;
  raft: CachedDataReducerState<api.RaftDebugResponseMessage>;
  version: CachedDataReducerState<VersionList>;
  databases: CachedDataReducerState<api.DatabasesResponseMessage>;
  databaseDetails: KeyedCachedDataReducerState<api.DatabaseDetailsResponseMessage>;
  tableDetails: KeyedCachedDataReducerState<api.TableDetailsResponseMessage>;
  tableStats: KeyedCachedDataReducerState<api.TableStatsResponseMessage>;
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
});

export {CachedDataReducerState, KeyedCachedDataReducerState};
