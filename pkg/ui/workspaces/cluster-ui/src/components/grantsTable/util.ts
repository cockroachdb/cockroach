// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

type FlatGrant = {
  grantee: string;
  privilege: string;
};
export const groupGrantsByGrantee = (grants: FlatGrant[]) => {
  if (!grants?.length) {
    return [];
  }

  const grantsByUser = {} as Record<string, string[]>;
  grants.forEach(grant => {
    if (!grantsByUser[grant.grantee]) {
      grantsByUser[grant.grantee] = [];
    }
    grantsByUser[grant.grantee].push(grant.privilege);
  });

  return Object.entries(grantsByUser).map(([grantee, privileges]) => ({
    key: grantee,
    grantee,
    privileges,
  }));
};
