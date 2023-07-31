// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

type JSONValue =
  | null
  | boolean
  | number
  | string
  | JSONValue[]
  | { [key: string]: JSONValue };

interface Logger {
  /**
   * Logs a message with additional context and an optional error at debug
   * level.
   * @param msg - the string message to log
   * @param context - additional structured context to include with the
   * message when shipped to a remote service
   * @param error - a possible JS Error to include with the message when
   * shipped to a remote service. Typed as unknown for convenience but
   * anything passed here which is not instanceof Error will not be attached.
   */
  debug(
    msg: string,
    context?: Record<string, JSONValue>,
    error?: unknown,
  ): void;
  /**
   * Logs a message with additional context and an optional error at info
   * level.
   * @param msg - the string message to log
   * @param context - additional structured context to include with the
   * message when shipped to a remote service
   * @param error - a possible JS Error to include with the message when
   * shipped to a remote service. Typed as unknown for convenience but
   * anything passed here which is not instanceof Error will not be attached.
   */
  info(msg: string, context?: Record<string, JSONValue>, error?: unknown): void;
  /**
   * Logs a message with additional context and an optional error at warn
   * level.
   * @param msg - the string message to log
   * @param context - additional structured context to include with the
   * message when shipped to a remote service
   * @param error - a possible JS Error to include with the message when
   * shipped to a remote service. Typed as unknown for convenience but
   * anything passed here which is not instanceof Error will not be attached.
   */
  warn(msg: string, context?: Record<string, JSONValue>, error?: unknown): void;
  /**
   * Logs a message with additional context and an optional error at error
   * level.
   * @param msg - the string message to log
   * @param context - additional structured context to include with the
   * message when shipped to a remote service
   * @param error - a possible JS Error to include with the message when
   * shipped to a remote service. Typed as unknown for convenience but
   * anything passed here which is not instanceof Error will not be attached.
   */
  error(
    msg: string,
    context?: Record<string, JSONValue>,
    error?: unknown,
  ): void;
}

let logger: Logger = console;

/**
 * Sets the logger returned by {@link getLogger}. It was added to allow
 * cockroach cloud to pass in a custom logger which attaches additional metadata
 * to each call and sends errors up to datadog.
 * @param newLogger the Logger to set
 */
export function setLogger(newLogger: Logger) {
  logger = newLogger;
}

/**
 * @returns the most recent logger set by {@link setLogger}, or console if one was never set.
 */
export function getLogger(): Logger {
  return logger;
}
