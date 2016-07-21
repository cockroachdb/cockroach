/**
 * This module maintains the state of read-only, data about databases and tables
 * Data is fetched from the '/_admin/v1/' endpoint via 'util/api'
 * Currently the data is always refreshed.
 */

import _ = require("lodash");
import { CachedDataReducer, CachedDataReducerState } from "./cachedDataReducer";
import { Action, PayloadAction, WithID } from "../interfaces/action";

export class KeyedCachedDataReducerState<TResponseMessage> {
  [id: string]: CachedDataReducerState<TResponseMessage>;
}

export class KeyedCachedDataReducer<TRequest, TResponseMessage> extends CachedDataReducer<TRequest, TResponseMessage> {
  constructor(protected apiEndpoint: (req: TRequest) => Promise<TResponseMessage>, public actionNamespace: string, private idGenerator: (req: TRequest) => string, protected invalidationPeriodMillis?: number) {
    super(apiEndpoint, actionNamespace, invalidationPeriodMillis);
    this.idGenerator = idGenerator;
  }

  // requestData is the REQUEST action creator.
  requestData = (req: TRequest): PayloadAction<WithID<void>> => {
    return {
      type: this.REQUEST,
      payload: {
        id: this.idGenerator(req),
      },
    };
  }

  // receiveData is the RECEIVE action creator.
  receiveData = (data: TResponseMessage, req: TRequest): PayloadAction<TResponseMessage|WithID<TResponseMessage>> => {
    return {
      type: this.RECEIVE,
      payload: {
        id: this.idGenerator(req),
        data,
      },
    };
  }

  // errorData is the ERROR action creator.
  errorData = (error: Error, req: TRequest): PayloadAction<WithID<Error>> => {
    return {
      type: this.ERROR,
      payload: {
        id: this.idGenerator(req),
        data: error,
      },
    };
  }

  // invalidateData is the INVALIDATE action creator.
  invalidateData = (req: TRequest): PayloadAction<WithID<void>> => {
    return {
      type: this.INVALIDATE,
      payload: {
        id: this.idGenerator(req),
      },
    };
  }

  keyedReducer = (state = new KeyedCachedDataReducerState<TResponseMessage>(), action: Action): KeyedCachedDataReducerState<TResponseMessage> => {
    switch (action.type) {
      case this.REQUEST:
      case this.RECEIVE:
      case this.ERROR:
      case this.INVALIDATE:
        let { id, data } = (action as PayloadAction<WithID<TResponseMessage|Error>>).payload;
        state = _.clone(state);
        state[id] = this.reducer(state[id], {
          type: action.type,
          payload: data,
        } as PayloadAction<typeof data>);
        return state;
      default:
        return state;
    }
  }
};
