import moment from 'moment'

export interface Identity {
  nodeID: number;
  address: string;
  locality?: string;
  updatedAt: moment.Moment;
}

export interface NoConnection {
  from: Identity;
  to: Identity;
}
