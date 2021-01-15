export const appAttr = "app";
export const dashQueryString = "dash";
export const dashboardNameAttr = "dashboard_name";
export const databaseNameAttr = "database_name";
export const implicitTxnAttr = "implicitTxn";
export const nodeIDAttr = "node_id";
export const nodeQueryString = "node";
export const rangeIDAttr = "range_id";
export const statementAttr = "statement";
export const tableNameAttr = "table_name";
export const sessionAttr = "session";

export const REMOTE_DEBUGGING_ERROR_TEXT =
  "This information is not available due to the current value of the 'server.remote_debugging.mode' setting.";

export const serverToClientErrorMessageMap = new Map([
  [
    "not allowed (due to the 'server.remote_debugging.mode' setting)",
    REMOTE_DEBUGGING_ERROR_TEXT,
  ],
]);
