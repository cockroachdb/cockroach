import identity from "lodash/identity";
import { createSelector } from "reselect";
import { AdminUIState } from "../state";

export const selectHotRanges = createSelector(
  identity,
  (state: AdminUIState) => state.hotRanges,
);
