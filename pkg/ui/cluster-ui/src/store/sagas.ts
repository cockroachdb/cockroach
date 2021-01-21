import { all, fork } from "redux-saga/effects";

import { localStorageSaga } from "./localStorage";
import { statementsDiagnosticsSagas } from "./statementDiagnostics";
import { statementsSaga } from "./statements";
import { nodesSaga } from "./nodes";
import { livenessSaga } from "./liveness";

export function* sagas(cacheInvalidationPeriod?: number) {
  yield all([
    fork(localStorageSaga),
    fork(statementsSaga, cacheInvalidationPeriod),
    fork(statementsDiagnosticsSagas, cacheInvalidationPeriod),
    fork(nodesSaga, cacheInvalidationPeriod),
    fork(livenessSaga, cacheInvalidationPeriod),
  ]);
}
