import { createSelector } from "@reduxjs/toolkit";
import { AppState } from "../reducers";

const livenessesSelector = (state: AppState) => state.adminUI.liveness.data;

export const livenessStatusByNodeIDSelector = createSelector(
  livenessesSelector,
  livenesses => livenesses?.statuses || {},
);
