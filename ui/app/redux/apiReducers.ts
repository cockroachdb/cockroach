import _ = require("lodash");
import { combineReducers } from "redux";
import { CachedDataReducer, CachedDataReducerState, KeyedCachedDataReducer, KeyedCachedDataReducerState } from "./cachedDataReducer";
import * as api from "../util/api";
import { VersionList } from "../interfaces/cockroachlabs";
import { versionCheck, VersionCheckRequest } from "../util/cockroachlabsAPI";
import { NodeStatus, RollupStoreMetrics } from "../util/proto";

type ClusterResponseMessage = cockroach.server.serverpb.ClusterResponseMessage;
type EventsRequest = cockroach.server.serverpb.EventsRequest;
type EventsResponseMessage = cockroach.server.serverpb.EventsResponseMessage;
type HealthResponseMessage = cockroach.server.serverpb.HealthResponseMessage;
type RaftDebugResponseMessage = cockroach.server.serverpb.RaftDebugResponseMessage;
type NodesResponseMessage = cockroach.server.serverpb.NodesResponseMessage;
type DatabasesResponseMessage = cockroach.server.serverpb.DatabasesResponseMessage;
type DatabaseDetailsRequest = cockroach.server.serverpb.DatabaseDetailsRequest;
type DatabaseDetailsResponseMessage = cockroach.server.serverpb.DatabaseDetailsResponseMessage;
type TableDetailsRequest= cockroach.server.serverpb.TableDetailsRequest;
type TableDetailsResponseMessage = cockroach.server.serverpb.TableDetailsResponseMessage;

const TEN_SECONDS = 10 * 1000;
const TWO_SECONDS = 2 * 1000;

const clusterReducerObj = new CachedDataReducer<void, ClusterResponseMessage>(api.getCluster, "cluster");
export const refreshCluster = clusterReducerObj.refresh;

const eventsReducerObj = new CachedDataReducer<EventsRequest, EventsResponseMessage>(api.getEvents, "events", TEN_SECONDS);
export const refreshEvents = eventsReducerObj.refresh;

export type HealthState = CachedDataReducerState<HealthResponseMessage>;
const healthReducerObj = new CachedDataReducer<void, HealthResponseMessage>(api.getHealth, "health", TWO_SECONDS);
export const refreshHealth = healthReducerObj.refresh;

function rollupStoreMetrics(res: NodesResponseMessage): NodeStatus[] {
  return _.map(res.nodes, (node) => {
    RollupStoreMetrics(node);
    return node;
  });
}

const nodesReducerObj = new CachedDataReducer<void, NodeStatus[]>(() => api.getNodes().then(rollupStoreMetrics), "nodes", TEN_SECONDS);
export const refreshNodes = nodesReducerObj.refresh;

const raftReducerObj = new CachedDataReducer<void, RaftDebugResponseMessage>(api.raftDebug, "raft");
export const refreshRaft = raftReducerObj.refresh;

const versionReducerObj = new CachedDataReducer<VersionCheckRequest, VersionList>(versionCheck, "version");
export const refreshVersion = versionReducerObj.refresh;

const databasesReducerObj = new CachedDataReducer<void, DatabasesResponseMessage>(api.getDatabaseList, "databases");
export const refreshDatabases = databasesReducerObj.refresh;

export const databaseRequestToID = (req: DatabaseDetailsRequest): string => req.database;

const databaseDetailsReducerObj = new KeyedCachedDataReducer<DatabaseDetailsRequest, DatabaseDetailsResponseMessage>(api.getDatabaseDetails, "databaseDetails", databaseRequestToID);
export const refreshDatabaseDetails = databaseDetailsReducerObj.refresh;

// NOTE: We encode the db and table name so we can combine them as a string.
// TODO(maxlang): there's probably a nicer way to do this
export function generateTableID(db: string, table: string) {
  return `${encodeURIComponent(db)}/${encodeURIComponent(table)}`;
}

export const tableRequestToID = (req: TableDetailsRequest): string => generateTableID(req.database, req.table);

const tableDetailsReducerObj = new KeyedCachedDataReducer<TableDetailsRequest, TableDetailsResponseMessage>(api.getTableDetails, "tableDetails", tableRequestToID);
export const refreshTableDetails = tableDetailsReducerObj.refresh;

export interface APIReducersState {
  cluster: CachedDataReducerState<ClusterResponseMessage>;
  events: CachedDataReducerState<EventsResponseMessage>;
  health: HealthState;
  nodes: CachedDataReducerState<NodeStatus[]>;
  raft: CachedDataReducerState<RaftDebugResponseMessage>;
  version: CachedDataReducerState<VersionList>;
  databases: CachedDataReducerState<DatabasesResponseMessage>;
  databaseDetails: KeyedCachedDataReducerState<DatabaseDetailsResponseMessage>;
  tableDetails: KeyedCachedDataReducerState<TableDetailsResponseMessage>;
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
});
