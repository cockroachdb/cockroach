import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";

export enum AggregationLevel {
  Cluster = "cluster",
  Node = "node",
}

export const SET_AGGREGATION_LEVEL = "cockroachui/AggregationLevel/SET";

const aggregationLevelSetting = new LocalSetting<AdminUIState, AggregationLevel>(
  "aggregation_level/setting", s => s.localSettings, AggregationLevel.Cluster,
);

export function setAggregationLevel(aggregationLevel:  AggregationLevel) {
  return aggregationLevelSetting.set(aggregationLevel);
}

export function selectAggregationLevel(state: AdminUIState) {
  return aggregationLevelSetting.selector(state);
}
