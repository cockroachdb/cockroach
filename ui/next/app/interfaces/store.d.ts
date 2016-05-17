import { NodeStatusState } from "../redux/nodes";
import { UISettingsDict } from "../redux/ui";
import { DatabaseInfoState } from "../redux/databaseInfo";

export interface AdminUIStore {
  // Nodes status query.
  nodes: NodeStatusState;
  // UI Settings.
  ui: UISettingsDict;
  // React-router-redux (we don't need access to this).
  routing: any;
  // database info
  databaseInfo: DatabaseInfoState;
}
