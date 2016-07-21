import _ = require("lodash");
import { combineReducers } from "redux";
import { CachedDataReducer, CachedDataReducerState } from "./cachedDataReducer";
import { KeyedCachedDataReducer, KeyedCachedDataReducerState } from "./keyedCachedDataReducer";
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

export let clusterReducerObj = new CachedDataReducer<void, ClusterResponseMessage>(api.getCluster, "cluster");
export let refreshCluster = clusterReducerObj.refresh;

export let eventsReducerObj = new CachedDataReducer<EventsRequest, EventsResponseMessage>(api.getEvents, "events", TEN_SECONDS);
export let refreshEvents = eventsReducerObj.refresh;

export type HealthState = CachedDataReducerState<HealthResponseMessage>;
export let healthReducerObj = new CachedDataReducer<void, HealthResponseMessage>(api.getHealth, "health", TWO_SECONDS);
export let refreshHealth = healthReducerObj.refresh;

function rollupStoreMetrics(res: NodesResponseMessage): NodeStatus[] {
  return _.map(res.nodes, (node) => {
    RollupStoreMetrics(node);
    return node;
  });
}

export let nodesReducerObj = new CachedDataReducer<void, NodeStatus[]>(() => api.getNodes().then(rollupStoreMetrics), "nodes", TEN_SECONDS);
export let refreshNodes = nodesReducerObj.refresh;

export let raftReducerObj = new CachedDataReducer<void, RaftDebugResponseMessage>(api.raftDebug, "raft");
export let refreshRaft = raftReducerObj.refresh;

export let versionReducerObj = new CachedDataReducer<VersionCheckRequest, VersionList>(versionCheck, "version");
export let refreshVersion = versionReducerObj.refresh;

export let databasesReducerObj = new CachedDataReducer<void, DatabasesResponseMessage>(api.getDatabaseList, "databases");
export let refreshDatabases = databasesReducerObj.refresh;

let databaseRequestToID = (req: DatabaseDetailsRequest): string => req.database;

export let databaseDetailsReducerObj = new KeyedCachedDataReducer<DatabaseDetailsRequest, DatabaseDetailsResponseMessage>(api.getDatabaseDetails, "databaseDetails", databaseRequestToID);
export let refreshDatabaseDetails = databaseDetailsReducerObj.refresh;

// NOTE: We encode the db and table name so we can combine them as a string.
// TODO(maxlang): there's probably a nicer way to do this
export function generateTableID(db: string, table: string) {
  return `${encodeURIComponent(db)}/${encodeURIComponent(table)}`;
}

let tableRequestToID = (req: TableDetailsRequest): string => generateTableID(req.database, req.table);

export let tableDetailsReducerObj = new KeyedCachedDataReducer<TableDetailsRequest, TableDetailsResponseMessage>(api.getTableDetails, "tableDetails", tableRequestToID);
export let refreshTableDetails = tableDetailsReducerObj.refresh;

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
  [databaseDetailsReducerObj.actionNamespace]: databaseDetailsReducerObj.keyedReducer,
  [tableDetailsReducerObj.actionNamespace]: tableDetailsReducerObj.keyedReducer,
});
