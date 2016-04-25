/// <reference path="../../typings/main.d.ts" />

export interface Action {
  type: string;
}

export interface PayloadAction<T> extends Action {
  payload: T;
}
