import { all, fork } from "redux-saga/effects";

import { localStorageSaga } from "./localStorage";
import { statementsDiagnosticsSagas } from "./statementDiagnostics";
import { statementsSaga } from "./statements";

export function* sagas(cacheInvalidationPeriod?: number) {
  yield all([
    fork(localStorageSaga),
    fork(statementsSaga, cacheInvalidationPeriod),
    fork(statementsDiagnosticsSagas, cacheInvalidationPeriod),
  ]);
}
