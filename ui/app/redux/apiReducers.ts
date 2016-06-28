import { CachedDataReducer, CachedDataReducerState } from "./cachedDataReducer";
import { getCluster, getHealth, getEvents, raftDebug } from "../util/api";

type ClusterResponseMessage = cockroach.server.serverpb.ClusterResponseMessage;
type EventsRequest = cockroach.server.serverpb.EventsRequest;
type EventsResponseMessage = cockroach.server.serverpb.EventsResponseMessage;
type HealthResponseMessage = cockroach.server.serverpb.HealthResponseMessage;
type RaftDebugResponseMessage = cockroach.server.serverpb.RaftDebugResponseMessage;

export type ClusterState = CachedDataReducerState<ClusterResponseMessage>;
export let clusterReducerObj = new CachedDataReducer<void, ClusterResponseMessage>(getCluster, "cluster");
export let clusterReducer = clusterReducerObj.reducer;
export let refreshCluster = clusterReducerObj.refresh;

export type EventsState = CachedDataReducerState<EventsResponseMessage>;
export let eventsReducerObj = new CachedDataReducer<EventsRequest, EventsResponseMessage>(getEvents, "events", 10000);
export let eventsReducer = eventsReducerObj.reducer;
export let refreshEvents = eventsReducerObj.refresh;

export type HealthState = CachedDataReducerState<HealthResponseMessage>;
export let healthReducerObj = new CachedDataReducer<void, HealthResponseMessage>(getHealth, "health", 2000);
export let healthReducer = healthReducerObj.reducer;
export let refreshHealth = healthReducerObj.refresh;

export type RaftDebugState = CachedDataReducerState<RaftDebugResponseMessage>;
export let raftReducerObj = new CachedDataReducer<void, RaftDebugResponseMessage>(raftDebug, "raft");
export let raftReducer = raftReducerObj.reducer;
export let refreshRaft = raftReducerObj.refresh;
