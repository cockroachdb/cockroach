// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

export const remoteNodeIDCookieName = "remote_node_id";

// currentNodeIDCookie will either be empty or contain two elements
// with the cookie name and value we're looking for
export const getCurrentNodeIDCookie = (): string[] => {
  return document.cookie
    .split(";")
    .map(cookieString => {
      return cookieString.split("=").map(kv => {
        return kv.trim();
      });
    })
    .find(cookie => {
      return cookie[0] === remoteNodeIDCookieName;
    });
};
