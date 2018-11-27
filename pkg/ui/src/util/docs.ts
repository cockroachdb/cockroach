// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import { getDataFromServer } from "src/util/dataFromServer";

const version = getDataFromServer().Version || "stable";
const docsURLBase = "https://www.cockroachlabs.com/docs/" + version;

function docsURL(pageName: string): string {
  return `${docsURLBase}/${pageName}`;
}

export const adminUILogin = docsURL("admin-ui-access-and-navigate.html#secure-the-admin-ui");
export const startFlags = docsURL("start-a-node.html#flags");
export const pauseJob = docsURL("pause-job.html");
export const cancelJob = docsURL("cancel-job.html");
export const enableNodeMap = docsURL("enable-node-map.html");
export const configureReplicationZones = docsURL("configure-replication-zones.html");

// Note that these explicitly don't use the current version, since we want to
// link to the most up-to-date documentation available.
export const upgradeCockroachVersion = "https://www.cockroachlabs.com/docs/stable/upgrade-cockroach-version.html";
export const enterpriseLicensing = "https://www.cockroachlabs.com/docs/stable/enterprise-licensing.html";

// Not actually a docs URL.
export const startTrial = "https://www.cockroachlabs.com/pricing/start-trial/";
