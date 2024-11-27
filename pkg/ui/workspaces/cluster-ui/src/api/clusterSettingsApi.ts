// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";

import { fetchData } from "src/api";

import { TimestampToMoment, useSwrImmutableWithClusterId } from "../util";

import { ADMIN_API_PREFIX } from "./util";

export type SettingsRequestMessage = cockroach.server.serverpb.SettingsRequest;
export type SettingsResponseMessage =
  cockroach.server.serverpb.SettingsResponse;

// getClusterSettings gets all cluster settings. We request unredacted_values, which will attempt
// to obtain all values from the server. The server will only accept to do so if
// the user also happens to have admin privilege.
export function getClusterSettings(
  req: SettingsRequestMessage,
  timeout: string,
): Promise<SettingsResponseMessage> {
  const urlParams = new URLSearchParams();
  urlParams.append("unredacted_values", "true");
  if (req.keys) {
    urlParams.append("keys", req.keys.join(","));
  }

  return fetchData(
    cockroach.server.serverpb.SettingsResponse,
    `${ADMIN_API_PREFIX}/settings?` + urlParams.toString(),
    null,
    null,
    timeout,
  );
}

// Usage of this request with the useClusterSettings hook
// is preferred over using getClusterSettings and its corresponding
// redux values and sagas.
export type GetClusterSettingRequest = {
  names: string[];
};

enum ClusterSettingType {
  BOOLEAN = "b",
  DURATION = "d",
  BYTE_SIZE = "z",
  INTEGER = "i",
  ENUM = "e",
  FLOAT = "f,",
  STRING = "s",
  VERSION = "m",
  UNKNOWN = "",
}

export type ClusterSetting = {
  name: string;
  value: string;
  type: ClusterSettingType;
  description: string;
  lastUpdated: moment.Moment | null;
};

const formatProtoResponse = (
  data: SettingsResponseMessage,
): Record<string, ClusterSetting> => {
  const settingsMap = {} as Record<string, ClusterSetting>;
  const entries = Object.values(data?.key_values ?? {});

  entries?.forEach(kv => {
    settingsMap[kv.name] = {
      name: kv.name,
      value: kv.value,
      type: kv.type as ClusterSettingType,
      description: kv.description,
      lastUpdated: TimestampToMoment(kv.last_updated),
    };
  });

  return settingsMap;
};

const emptyClusterSetting: ClusterSetting = {
  name: "",
  value: "",
  type: ClusterSettingType.UNKNOWN,
  description: "",
  lastUpdated: null,
};

export const useClusterSettings = (req: GetClusterSettingRequest) => {
  const protoReq = new cockroach.server.serverpb.SettingsRequest({
    keys: req.names,
  });
  const { data, isLoading, error } = useSwrImmutableWithClusterId(
    {
      name: "clusterSettings",
      settings: req.names,
    },
    () => getClusterSettings(protoReq, "1M").then(formatProtoResponse),
  );

  // If we don't have data we'll return a map of empty settings.
  const settingValues =
    data ??
    req.names.reduce(
      (acc, name) => {
        acc[name] = { ...emptyClusterSetting };
        return acc;
      },
      {} as Record<string, ClusterSetting>,
    );

  return {
    settingValues,
    isLoading,
    error,
  };
};
