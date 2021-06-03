import { createSelector } from "reselect";

import { adminUISelector } from "../statementsPage/statementsPage.selectors";

export const selectTransactionsSlice = createSelector(
  adminUISelector,
  adminUiState => adminUiState.transactions,
);

export const selectTransactionsData = createSelector(
  selectTransactionsSlice,
  transactionsState => transactionsState.data,
);

export const selectTransactionsLastError = createSelector(
  selectTransactionsSlice,
  state => state.lastError,
);
