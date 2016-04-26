/**
 * Action is the interface that should be implemented by all redux actions in
 * this application.
 */
export interface Action {
  type: string;
}

/**
 * PayloadAction implements the very common case of an action that includes a
 * single data object as a payload.
 */
export interface PayloadAction<T> extends Action {
  payload: T;
}
