// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import * as d3 from "d3";

import * as vector from "src/util/vector";

// createArcPath returns an svg arc object. startAngle and endAngle are
// expressed in radians.
export function createArcPath(
  innerR: number,
  outerR: number,
  startAngle: number,
  endAngle: number,
  cornerRadius: number,
) {
  return d3.svg
    .arc()
    .innerRadius(innerR)
    .outerRadius(outerR)
    .cornerRadius(cornerRadius)
    .startAngle(startAngle)
    .endAngle(endAngle)(null);
}

export function arcAngleFromPct(pct: number) {
  return Math.PI * (pct * 1.25 - 0.625);
}

export function angleFromPct(pct: number) {
  return Math.PI * (-1.25 + 1.25 * pct);
}

// findClosestPoint locates the closest point on the vector starting
// from point s and extending through u (t=1), nearest to point p.
// Returns an empty vector if the closest point is either start or end
// point or located before or after the line segment defined by [s,
// e].
export function findClosestPoint(
  s: [number, number],
  e: [number, number],
  p: [number, number],
): [number, number] {
  // u = e - s
  // v = s+tu - p
  // d = length(v)
  // d = length((s-p) + tu)
  // d = sqrt(([s-p].x + tu.x)^2 + ([s-p].y + tu.y)^2)
  // d = sqrt([s-p].x^2 + 2[s-p].x*tu.x + t^2u.x^2 + [s-p].y^2 + 2[s-p].y*tu.y + t^2*u.y^2)
  // ...minimize with first derivative with respect to t
  // 0 = 2[s-p].x*u.x + 2tu.x^2 + 2[s-p].y*u.y + 2tu.y^2
  // 0 = [s-p].x*u.x + tu.x^2 + [s-p].y*u.y + tu.y^2
  // t*(u.x^2 + u.y^2) = [s-p].x*u.x + [s-p].y*u.y
  // t = ([s-p].x*u.x + [s-p].y*u.y) / (u.x^2 + u.y^2)
  const u = vector.sub(e, s);
  const d = vector.sub(s, p);
  const t = -(d[0] * u[0] + d[1] * u[1]) / (u[0] * u[0] + u[1] * u[1]);
  if (t <= 0 || t >= 1) {
    return [0, 0];
  }
  return vector.add(s, vector.mult(u, t));
}
