// source: models/timescale.ts

/// <reference path="../../typings/browser.d.ts" />

module Models {
  "use strict";
  export module Timescale {
    export interface Timescale {
      name: string;
      scale: number;
      selected?: boolean;
    };

    export function getScale(amount: number, unit: string): number {
      return moment.duration(amount, unit).asMilliseconds();
    }

    export let timescales: Timescale[] = [
      { name: "10 min", scale: getScale(10, "minutes"), selected: true },
      { name: "1 hour", scale: getScale(1, "hour") },
      { name: "6 hours", scale: getScale(6, "hours") },
      { name: "12 hours", scale: getScale(12, "hours") },
      { name: "1 day", scale: getScale(1, "day") },
      // TODO: handle higher time scales
    ];

    export function setTimescale(t: Timescale): void {
      _.each(timescales, (ts: Timescale): void => { ts.selected = false; });
      t.selected = true;
      // TODO: refresh graphs
    }

    export let getCurrentTimescale: () => number = (): number => (_.find(timescales, { selected: true }) || _.first(timescales)).scale;
  }
}
