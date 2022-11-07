// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

let path = "";

/**
 * Sets the base URL to use that all API paths are appended to in
 * the app. When running Cluster UI components embedded elsewhere, this is
 * helpful to ensure that requests are routed to your particular cluster when
 * it's not served from the Base URL of your application. This path should
 * **not** include a trailing slash.
 */
export const setBasePath = (basePath: string): string => (path = basePath);

export const getBasePath = (): string => path;
