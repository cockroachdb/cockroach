import {CachedDataReducer} from "src/redux/cachedDataReducer";
import * as api from "../../api";
import moment from "moment";

export const statementDiagnosticsReportsReducerObj = new CachedDataReducer(
  api.getStatementDiagnosticsReports,
  "statementDiagnosticsReports",
  moment.duration(5, "m"),
  moment.duration(1, "m"),
);
export const refreshStatementDiagnosticsRequests = statementDiagnosticsReportsReducerObj.refresh;
export const invalidateStatementDiagnosticsRequests = statementDiagnosticsReportsReducerObj.invalidateData;
