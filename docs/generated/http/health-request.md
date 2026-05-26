

<a name="cockroach.server.serverpb.HealthRequest"></a>
#### HealthRequest

HealthRequest requests a liveness or readiness check.

A liveness check is triggered via ready set to false. In this mode,
an empty response is returned immediately, that is, the caller merely
learns that the process is running.

A readiness check (ready == true) is suitable for determining whether
user traffic should be directed at a given node, for example by a load
balancer. In this mode, a successful response is returned only if the
node:

- is not in the process of shutting down or booting up (including
  waiting for cluster bootstrap);
- is regarded as healthy by the cluster via the recent broadcast of
  a liveness beacon. Absent either of these conditions, an error
  code will result.

Support status: [public](#support-status)


| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
| ready | [bool](#bool) |  | ready specifies whether the client wants to know whether the target node is ready to receive traffic. If a node is unready, an error will be returned. | [public](#support-status) |


