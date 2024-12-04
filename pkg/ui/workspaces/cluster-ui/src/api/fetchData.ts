// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { RequestError } from "../util";

import { withBasePath } from "./basePath";

interface ProtoBuilder<
  P extends ConstructorType,
  Prop = FirstConstructorParameter<P>,
  R = InstanceType<P>,
> {
  new (properties?: Prop): R;
  encode(message: Prop, writer?: protobuf.Writer): protobuf.Writer;
  decode(reader: protobuf.Reader | Uint8Array, length?: number): R;
}

export function toArrayBuffer(encodedRequest: Uint8Array): ArrayBuffer {
  return encodedRequest.buffer.slice(
    encodedRequest.byteOffset,
    encodedRequest.byteOffset + encodedRequest.byteLength,
  );
}

/**
 * @param respBuilder expects protobuf stub class to build decode response;
 * @param path relative URL path for requested resource;
 * @param reqBuilder expects protobuf stub to encode request payload. It has to be
 * class type, not instance;
 * @param reqPayload is request payload object;
 * @param timeout is the timeout for the request (optional),
 * format is TimeoutValue (positive integer of at most 8 digits) +
 * TimeoutUnit ( Hour → "H", Minute → "M", Second → "S", Millisecond → "m" ),
 * e.g. "1M" (1 minute), default value "30S" (30 seconds);
 **/
export const fetchData = <P extends ProtoBuilder<P>, T extends ProtoBuilder<T>>(
  respBuilder: T,
  path: string,
  reqBuilder?: P,
  reqPayload?: ConstructorParameters<P>[0],
  timeout?: string,
): Promise<InstanceType<T>> => {
  const grpcTimeout = timeout || "30S";
  const params: RequestInit = {
    headers: {
      Accept: "application/x-protobuf",
      "Content-Type": "application/x-protobuf",
      "Grpc-Timeout": grpcTimeout,
    },
    credentials: "same-origin",
  };

  if (reqPayload) {
    const encodedRequest = reqBuilder.encode(reqPayload).finish();
    params.method = "POST";
    params.body = toArrayBuffer(encodedRequest);
  }

  return fetch(withBasePath(path), params)
    .then(response => {
      if (!response.ok) {
        return getErrorBodyFromResponse(response).then(errorBody => {
          throw new RequestError(
            response.status,
            errorBody || response.statusText,
          );
        });
      }
      return response.arrayBuffer();
    })
    .then(buffer => respBuilder.decode(new Uint8Array(buffer)));
};

/**
 * fetchDataJSON makes a request for /api/v2 which uses content type JSON.
 * @param path relative path for requested resource.
 * @param reqPayload request payload object.
 */
export function fetchDataJSON<ResponseType, RequestType>(
  path: string,
  reqPayload?: RequestType,
): Promise<ResponseType> {
  const params: RequestInit = {
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
      "X-Cockroach-API-Session": "cookie",
    },
    credentials: "same-origin",
  };

  if (reqPayload) {
    params.method = "POST";
    params.body = JSON.stringify(reqPayload);
  }

  return fetch(withBasePath(path), params).then(response => {
    if (!response.ok) {
      return getErrorBodyFromResponse(response).then(errorBody => {
        throw new RequestError(
          response.status,
          errorBody || response.statusText,
        );
      });
    }

    return response.json();
  });
}

async function getErrorBodyFromResponse(response: Response): Promise<string> {
  const contentType = response.headers.get("Content-Type");
  try {
    if (contentType.includes("application/json")) {
      const json = await response.json();
      return JSON.stringify(json);
    }

    if (contentType.includes("text/plain")) {
      return response.text();
    }

    if (contentType.includes("application/x-protobuf")) {
      const buffer = await response.arrayBuffer();
      const error = cockroach.server.serverpb.ResponseError.decode(
        new Uint8Array(buffer),
      );
      return error.error;
    }
  } catch {
    // If we can't parse the error body, we'll just return the status text.
    // Note that statusText is not available in http2 responses so we'll fall
    // back to status code.
  }

  return Promise.resolve(response.statusText || response.status.toString());
}
