// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

export const getCurrentNodeIDFromCookie = (cookieName: string): string[] => {
  return document.cookie
    .split(";")
    .map(cookieString => {
      return cookieString.split("=").map(kv => {
        return kv.trim();
      });
    })
    .find(cookie => {
      return cookie[0] === cookieName;
    });
};

export const selectTenantsFromCookie = (): string[] => {
  const sessionCookieArr = document.cookie
    .split(";")
    .filter(row => row.trim().startsWith("session="));
  return sessionCookieArr.length > 0
    ? sessionCookieArr[0]
        .replace(/["]/g, "")
        .replace("session=", "")
        .split(/[,&]/g)
        .filter((_, idx) => idx % 2 == 1)
    : [];
};

export const selectCurrentTenantFromCookie = (): string | null => {
  const tenantCookieStr = document.cookie
    .split(";")
    .filter(row => row.trim().startsWith("tenant="))[0];
  return tenantCookieStr ? tenantCookieStr.replace("tenant=", "") : null;
};

export const setCookie = (
  key: string,
  val: string,
  expires?: string,
  path?: string,
) => {
  let cookieStr = `${key}=${val};`;
  if (expires) {
    cookieStr += `expires=${expires};`;
  }
  if (path) {
    cookieStr += `path=${path}`;
  } else {
    cookieStr += "/";
  }
  document.cookie = cookieStr;
};
