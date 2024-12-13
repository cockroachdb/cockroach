// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import fetchMock from "jest-fetch-mock";

import { RequestError } from "src/util";

import { fetchDataJSON, fetchData } from "./fetchData";

describe("fetchDataJSON", () => {
  beforeAll(fetchMock.enableMocks);
  beforeEach(() => {
    fetchMock.resetMocks();
  });
  afterAll(fetchMock.disableMocks);

  it("should fetch data from APIs returning JSON", async () => {
    // Mock the fetch function to return a successful response.
    fetchMock.mockResponseOnce(JSON.stringify({ data: "data" }), {
      status: 200,
    });

    const data = await fetchDataJSON("json");
    expect(data).toEqual({ data: "data" });
  });

  it.each([
    {
      description: "500 status with status text",
      response: { status: 500, statusText: "error1" },
      body: "",
      expected: new RequestError(500, "error1"),
    },
    {
      description: "500 status with plain text error",
      response: { status: 500, headers: { "Content-Type": "text/plain" } },
      body: "error2",
      expected: new RequestError(500, "error2"),
    },
    {
      description: "500 status with JSON error",
      response: {
        status: 500,
        headers: { "Content-Type": "application/json" },
      },
      body: JSON.stringify({ error: "error" }),
      expected: new RequestError(500, JSON.stringify({ error: "error" })),
    },
    {
      description: "401 status with JSON error on POST",
      request: { data: "data" },
      response: {
        status: 401,
        headers: { "Content-Type": "application/json" },
      },
      body: JSON.stringify({ error: "jsonError2" }),
      expected: new RequestError(401, JSON.stringify({ error: "jsonError2" })),
    },
  ])(
    "should throw error when encountering non-200 status: $description",
    async ({ request, response, body, expected }) => {
      fetchMock.mockResponse(() =>
        Promise.resolve({
          body: body as string,
          init: { ...response },
        }),
      );

      try {
        await fetchDataJSON("fetchJson", request);
      } catch (e) {
        const err = e as RequestError;
        expect(err.status).toBe(response.status);
        expect(err.message).toBe(expected.message);
      }
    },
  );
});

describe("fetchData", () => {
  beforeAll(fetchMock.enableMocks);
  beforeEach(() => {
    fetchMock.resetMocks();
  });
  afterAll(fetchMock.disableMocks);

  it("should fetch data and decode response from protobuf API", async () => {
    const mockResp = uint8ArrayToString(
      cockroach.server.serverpb.SettingsResponse.encode({
        key_values: { key: { value: "data" } },
      }).finish(),
    );
    fetchMock.mockResponseOnce(mockResp, {
      status: 200,
      headers: { "Content-Type": "application/x-protobuf" },
    });

    const data = await fetchData(
      cockroach.server.serverpb.SettingsResponse,
      "api/protobuf",
    );
    expect(data.key_values).toEqual({ key: { value: "data" } });
  });

  it.each([
    {
      description: "500 status with protobuf error",
      response: {
        status: 500,
        headers: { "Content-Type": "application/x-protobuf" },
      },
      body: "Error in response",
      expected: new RequestError(500, "Error in response"),
    },
    {
      description: "401 status with protobuf error on POST",
      request: {},
      response: {
        status: 401,
        headers: { "Content-Type": "application/x-protobuf" },
      },
      body: "Error in response",
      expected: new RequestError(401, "Error in response"),
    },
  ])(
    "should throw error when encountering non-200 status: $description",
    async ({ request, response, body, expected }) => {
      const mockResp = uint8ArrayToString(
        cockroach.server.serverpb.ResponseError.encode({
          error: body,
          message: "Error in response",
        }).finish(),
      );

      fetchMock.mockResponseOnce(mockResp, {
        status: response.status,
        headers: { "Content-Type": "application/x-protobuf" },
      });

      try {
        await fetchData(
          cockroach.server.serverpb.SettingsResponse,
          "api/protobuf",
          cockroach.server.serverpb.SettingsRequest,
          request,
        );
      } catch (e) {
        const err = e as RequestError;
        expect(err.status).toBe(response.status);
        expect(err.message).toBe(expected.message);
      }
    },
  );
});

function uint8ArrayToString(uint8Array: Uint8Array): string {
  return String.fromCharCode(...Array.from(uint8Array));
}
