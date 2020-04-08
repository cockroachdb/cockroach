import moment from "moment";
import {CachedDataReducer} from "src/redux/cachedDataReducer";
import * as api from "../../api";

export const statementsReducerObj = new CachedDataReducer(
  api.getStatements,
  "statements",
  moment.duration(5, "m"),
  moment.duration(1, "m"),
);

export const refreshStatements = statementsReducerObj.refresh;
