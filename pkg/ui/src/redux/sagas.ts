import { all, fork } from "redux-saga/effects";

import { queryMetricsSaga } from "src/redux/metrics";
import { watchRequestMetricsMetadata } from "src/redux/metricsMetadata";

export default function* rootSaga() {
  yield all([
    fork(queryMetricsSaga),
    fork(watchRequestMetricsMetadata),
  ]);
}
