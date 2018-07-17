import { Dashboard } from "./interface";
import { overviewDashboard } from "./overview";

export const allDashboards: { [name: string]: Dashboard } = {
  overview: overviewDashboard,
};
