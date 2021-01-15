import { AnyAction } from "redux";
import { all, call, takeEvery } from "redux-saga/effects";
import { actions } from "./localStorage.reducer";

export function* updateLocalStorageItemSaga(action: AnyAction) {
  const { key, value } = action.payload;
  yield call({ context: localStorage, fn: localStorage.setItem }, key, value);
}

export function* localStorageSaga() {
  yield all([takeEvery(actions.update, updateLocalStorageItemSaga)]);
}
