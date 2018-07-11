import { AdminUIState } from "src/redux/state";
import { setRouteParam } from "src/redux/navigator";

export enum AggregationLevel {
  Cluster = "cluster",
  Node = "node",
}

export const SET_AGGREGATION_LEVEL = "cockroachui/AggregationLevel/SET";

export function setAggregationLevel(aggregationLevel:  AggregationLevel) {
  if (aggregationLevel === AggregationLevel.Cluster) {
    return setRouteParam("agg", undefined);
  }

  return setRouteParam("agg", aggregationLevel);
}

export function selectAggregationLevel(state: AdminUIState) {
  return state.routing.locationBeforeTransitions.query.agg || AggregationLevel.Cluster;
}
