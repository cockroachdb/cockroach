import moment from "moment";

export interface Identity {
  nodeID: number;
  address: string;
  locality?: string;
  updatedAt: moment.Moment;
}
