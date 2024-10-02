// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
