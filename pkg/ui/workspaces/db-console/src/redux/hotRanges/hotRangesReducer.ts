// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
const generateFakeData = (data: any) => {
  const mockDataList = [
    {
      rangeId: "1",
      queriesPerSecond: 7.939165871329897,
      nodeIds: [1, 2, 3],
      leaseHolder: 1,
      database: "Movr_2go",
      table: "customer",
      index: "idx1_customer",
    },
    {
      rangeId: "2",
      queriesPerSecond: 5.182174823577275,
      nodeIds: [2, 3],
      leaseHolder: 2,
      database: "Movr_docks",
      table: "orders",
      index: "idx1_orders",
    },
    {
      rangeId: "3",
      queriesPerSecond: 1.0521625994622381,
      nodeIds: [3, 4, 5],
      leaseHolder: 4,
      database: "Movr_customers",
      table: "users",
      index: "idx1_vehicles",
    },
  ];
  return mockDataList;
};

export interface HotRange {
  rangeId: string;
  queriesPerSecond: number;
  nodeIds: number[];
  leaseHolder: number;
  database: string;
  table: string;
  index: string;
}
export interface HotRangesState {
  loading: boolean;
  data?: HotRange[];
  error?: Error;
}

const INITIAL_STATE = {
  loading: true,
};

export default function (state = INITIAL_STATE, action: any): HotRangesState {
  switch (action.type) {
    case "GET_HOT_RANGES_SUCCEEDED":
      return {
        loading: false,
        data: generateFakeData(action.payload),
      };
    case "GET_HOT_RANGES_FAILED":
      return {
        ...state,
        loading: false,
        error: action.payload,
      };
    default:
      return state;
  }
}
