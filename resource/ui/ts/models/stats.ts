// source: models/stats.ts
/// <reference path="../typings/mithriljs/mithril.d.ts" />
// Author: Bram Gruneir (bram.gruneir@gmail.com)

/**
 * Models contains data models pulled from cockroach.
 */
module Models {
    export module Stats {
        export interface MVCCStats {
            live_bytes: number;
            key_bytes: number;
            val_bytes: number;
            intent_bytes: number;
            live_count: number;
            key_count: number;
            val_count: number;
            intent_count: number;
            intent_age: number;
            gc_bytes_age: number;
            sys_bytes: number;
            sys_count: number;
            last_update_nanos: number;
        }
    }
}
