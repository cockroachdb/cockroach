// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { useMemo } from "react";

import { fetchData } from "src/api";

import { useSwrWithClusterId } from "../util";

type ILocation = cockroach.server.serverpb.LocationsResponse.ILocation;

const LOCATIONS_PATH = "_admin/v1/locations";
const LOCATIONS_SWR_KEY = "locations";

export interface LocationTree {
  [key: string]: {
    [value: string]: ILocation;
  };
}

const getLocations =
  (): Promise<cockroach.server.serverpb.LocationsResponse> => {
    return fetchData(
      cockroach.server.serverpb.LocationsResponse,
      LOCATIONS_PATH,
    );
  };

function nestLocations(data: ILocation[]): LocationTree {
  return data.reduce((acc: LocationTree, location: ILocation) => {
    const { locality_key: localityKey, locality_value: localityValue } =
      location;
    if (!acc[localityKey]) {
      acc[localityKey] = {};
    }
    acc[localityKey][localityValue] = location;
    return acc;
  }, {});
}

export const useLocations = () => {
  const { data, isLoading, error } = useSwrWithClusterId(
    LOCATIONS_SWR_KEY,
    getLocations,
    {
      revalidateOnFocus: false,
      dedupingInterval: 600_000, // 10 minutes, matching the old invalidation period.
    },
  );

  const locations = useMemo(() => data?.locations ?? [], [data]);

  const locationTree = useMemo(() => nestLocations(locations), [locations]);

  return {
    isLoading,
    error,
    locations,
    locationTree,
  };
};
