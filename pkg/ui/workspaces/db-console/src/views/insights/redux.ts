import { LocalSetting } from "src/redux/localsettings";
import { AdminUIState } from "src/redux/state";
import { createSelector } from "reselect";
import {
  InsightsTransactionFilters,
  defaultFilters,
  SortSetting,
} from "@cockroachlabs/cluster-ui";

export const filtersLocalSetting = new LocalSetting<
  AdminUIState,
  InsightsTransactionFilters
>(
  "filters/InsightsTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  { app: defaultFilters.app },
);

export const sortSettingLocalSetting = new LocalSetting<
  AdminUIState,
  SortSetting
>(
  "sortSetting/InsightsTransactionsPage",
  (state: AdminUIState) => state.localSettings,
  { ascending: false, columnTitle: "startTime" },
);

export const selectContentionTransactionEvents = createSelector(
  (state: AdminUIState) => state.cachedData,
  adminUiState => {
    if (!adminUiState.contentionTransactions) return [];
    return adminUiState.contentionTransactions.data;
  },
);
