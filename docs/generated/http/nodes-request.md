

<a name="cockroach.server.serverpb.NodesRequest"></a>
#### NodesRequest

NodesRequest requests a copy of the node information as known to gossip
and the KV layer.

Support status: [alpha](#support-status)


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| redact | [bool](#bool) |  | redact, if true, requests redaction of sensitive data away from the API response. | [reserved](#support-status) |


