// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";

import { fetchData } from "src/api";

import { TimestampToMoment, useSwrWithClusterId } from "../util";

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
  public: boolean;
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
      public: kv.public ?? false,
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
  public: false,
};

const CLUSTER_SETTINGS_SWR_KEY = "clusterSettings";

/**
 * useClusterSettings fetches all cluster settings via SWR. All callers
 * share the same cache entry regardless of the names requested.
 *
 * When `req` is provided, only the requested settings are returned
 * (with missing entries filled with defaults). When omitted, all
 * settings are returned.
 */
export const useClusterSettings = (req?: GetClusterSettingRequest) => {
  const protoReq = new cockroach.server.serverpb.SettingsRequest({});
  const { data, isLoading, error } = useSwrWithClusterId(
    CLUSTER_SETTINGS_SWR_KEY,
    () => getClusterSettings(protoReq, "1M").then(formatProtoResponse),
    {
      revalidateOnFocus: false,
      dedupingInterval: 30_000,
    },
  );

  const allSettings = data ?? {};

  if (!req?.names) {
    return { settingValues: allSettings, isLoading, error };
  }

  const settingValues = req.names.reduce(
    (acc, name) => {
      acc[name] = allSettings[name] ?? { ...emptyClusterSetting };
      return acc;
    },
    {} as Record<string, ClusterSetting>,
  );

  return { settingValues, isLoading, error };
};
