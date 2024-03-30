// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

export const SYSTEM_TENANT_NAME = "system";

export const getAllCookies = (): Map<string, string> => {
  const cookieMap: Map<string, string> = new Map();
  const cookiesArr = document.cookie.split(";");
  cookiesArr.map(cookie => {
    const i = cookie.indexOf("=");
    const keyValArr = [cookie.slice(0, i), cookie.slice(i + 1)];
    cookieMap.set(keyValArr[0].trim(), keyValArr[1].trim());
  });
  return cookieMap;
};

export const getCookieValue = (cookieName: string): string => {
  const cookies = getAllCookies();
  return cookies.get(cookieName) || null;
};

export const clearTenantCookie = () => {
  setCookie("tenant", "");
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
    cookieStr += document.location.pathname;
  }
  document.cookie = cookieStr;
};
