import { AdminUIState } from "src/redux/state";

export const hotRangesSelector = (state: AdminUIState) =>
  Object.values(state.cachedData.hotRanges)
    .filter(
      v => v.valid && !v.inFlight && !v.lastError && v.data?.ranges.length > 0,
    )
    .map(v => v.data.ranges)
    .reduce((acc, v) => {
      acc.push(...v);
      return acc;
    }, []);
