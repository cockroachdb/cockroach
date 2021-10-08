# Monitoring

This directory contains various sample configurations related to third party monitoring in CockroachDB.

## Developing Grafana Dashboards

### Bootstrap

Inside the `demo` directory lives a docker compose file used to bootstrap a three node cockroachdb cluster hooked up to
an instance of prometheus and grafana. This will allow for spinning up a quick dev environment for performing manual QA
or making any modifications on the grafana dashboards.

To get started, run this command to launch the containers:  
`$ cd demo && docker compose up`

The output of `$ docker ps` should resemble what is shown below:

```bash
CONTAINER ID   IMAGE          COMMAND                  CREATED          STATUS         PORTS                                                                                      NAMES
0f0f35180a8b   bc8c9ea5532e   "/run.sh"                18 minutes ago   Up 5 minutes   0.0.0.0:3000->3000/tcp, :::3000->3000/tcp                                                  demo_grafana_1
aba0ebd929ef   49cce1dff0e5   "/bin/prometheus --c…"   18 minutes ago   Up 5 minutes   0.0.0.0:9090->9090/tcp, :::9090->9090/tcp                                                  demo_prometheus_1
043bc21a74a0   bb9ac5c2ae52   "/cockroach/cockroac…"   18 minutes ago   Up 5 minutes   0.0.0.0:8080->8080/tcp, :::8080->8080/tcp, 0.0.0.0:26257->26257/tcp, :::26257->26257/tcp   demo_roach1_1
15c6e9c2167e   bb9ac5c2ae52   "/cockroach/cockroac…"   18 minutes ago   Up 5 minutes   8080/tcp, 26257/tcp                                                                        demo_roach2_1
0adb9e3b2c50   bb9ac5c2ae52   "/cockroach/cockroac…"   18 minutes ago   Up 5 minutes   8080/tcp, 26257/tcp                                                                        demo_roach3_1
```

Now that each container is up and running, we can access the web interface of each service.

- `localhost:8080`: cockroachdb console
- `localhost:9090`: prometheus web ui
- `localhost:3000`: grafana (dashboards located at `/dashboards`)

In addition, `localhost:26257` is accessible for sql connections targeting the first node.

### Making dashboard changes

Dashboard modifications happen in the Grafana web interface. To save changes, click on the "Save dashboard" icon in the
top right menu. Then proceed to export the dashboard as a JSON file by clicking the "Save JSON to file" button. Lastly,
move this file to the `grafana-dashboards` folder.

### Running a workload

Workloads can be used to generate activity and provide time series data for visualization. For example:  
`$ cockroach workload init movr`  
`$ cockroach workload run movr`

### Cleanup

`CTRL+C` will stop the running containers.  
`$ docker compose down` will remove all data from previous runs.
