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

/**
 * These types are used when communicating with Cockroach Labs servers. They are
 * based on https://github.com/cockroachlabs/registration/blob/master/db.go and
 * the requests expected in
 * https://github.com/cockroachlabs/registration/blob/master/http.go
 */

export interface Version {
  version: string;
  detail: string;
}

export interface VersionList {
  details: Version[];
}

export interface VersionStatus {
  error: boolean;
  message: string;
  laterVersions: VersionList;
}

interface RegistrationData {
  first_name: string;
  last_name: string;
  company: string;
  email: string;
  product_updates: boolean;
}

export interface VersionCheckRequest {
  clusterID: string;
  buildtag: string;
}
