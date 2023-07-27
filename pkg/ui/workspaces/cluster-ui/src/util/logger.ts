// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

type JSONPrimitive = string | number | boolean | null
type JSONValue = JSONPrimitive | JSONObject | JSONArray
type JSONObject = { [member: string]: JSONValue }
type JSONArray = JSONValue[]

interface Logger {
    debug(msg: string, context?: Record<string, JSONValue>, error?: unknown): void;
    info(msg: string, context?: Record<string, JSONValue>, error?: unknown): void;
    warn(msg: string, context?: Record<string, JSONValue>, error?: unknown): void;
    error(msg: string, context?: Record<string, JSONValue>, error?: unknown): void;
}

let logger: Logger = console;

// setLogger sets the logger returned by getLogger. It was added to allow
// cockroach cloud to pass in a custom logger which attaches additional metadata
// to call.
export function setLogger(newLogger: Logger) {
	logger = newLogger;
}

// getLogger returns the last logger set by setLogger.  If a logger hasn't been
// set, console is returned.
export function getLogger(): Logger {
	return logger;
}
