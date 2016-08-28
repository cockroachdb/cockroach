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

/**
 * WithRequest implements the very common case of an action payload that has an
 * associated Request.
 */
interface WithRequest<T, R> {
  data?: T;
  request: R;
}
