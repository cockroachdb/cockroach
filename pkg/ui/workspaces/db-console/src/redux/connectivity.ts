import { AdminUIState } from "src/redux/state";

export const connectivitySelector = (state: AdminUIState) =>
  state.cachedData.connectivity;
