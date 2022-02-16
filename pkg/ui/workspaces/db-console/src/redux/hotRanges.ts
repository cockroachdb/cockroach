import { AdminUIState } from "src/redux/state";
import { createSelector } from "reselect";
import { orderBy } from "lodash";

const hotRangesState = (state: AdminUIState) =>
  Object.values(state.cachedData.hotRanges);

const validHotRanges = createSelector(hotRangesState, hotRanges =>
  hotRanges.filter(v => v.valid && !v.inFlight),
);

export const hotRangesSelector = createSelector(validHotRanges, hotRanges =>
  hotRanges
    .filter(v => v.data?.ranges.length > 0)
    .map(v => v.data.ranges)
    .reduce((acc, v) => {
      acc.push(...v);
      return acc;
    }, []),
);

export const pagesWithErrorSelector = createSelector(
  validHotRanges,
  hotRanges => hotRanges.filter(v => !!v.lastError),
);

export const lastErrorSelector = createSelector(
  pagesWithErrorSelector,
  hotRanges => {
    const orderedPages = orderBy(hotRanges, ["setAt"], ["desc"]);
    if (orderedPages.length > 0) {
      return orderedPages[0].lastError;
    }
    return null;
  },
);

export const isValidSelector = createSelector(hotRangesState, hotRanges =>
  hotRanges.every(v => v.valid),
);

export const lastSetAtSelector = createSelector(
  pagesWithErrorSelector,
  hotRanges => {
    const orderedPages = orderBy(hotRanges, ["setAt"], ["desc"]);
    if (orderedPages.length > 0) {
      return orderedPages[0].setAt;
    }
    return null;
  },
);

export const isLoadingSelector = createSelector(hotRangesState, hotRanges =>
  hotRanges.some(v => v.inFlight),
);

