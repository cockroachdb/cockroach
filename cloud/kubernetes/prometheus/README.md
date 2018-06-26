This guide is based on using CoreOS's Prometheus Operator, which allows
a Prometheus instance to be managed using native Kubernetes concepts.


References used:
* https://github.com/coreos/prometheus-operator/blob/master/Documentation/user-guides/alerting.md
* https://github.com/coreos/prometheus-operator/blob/master/Documentation/user-guides/getting-started.md

# Preflight

Create and initialize a Cockroach cluster, if you haven't already done
so:
* `kubectl apply -f
https://raw.githubusercontent.com/cockroachdb/cockroach/master/cloud/kubernetes/v1.7/cockroachdb-statefulset.yaml`
* `kubectl apply -f
https://raw.githubusercontent.com/cockroachdb/cockroach/master/cloud/kubernetes/v1.7/cluster-init.yaml`


If you're running on Google Kubernetes Engine, it's necessary to ensure
that your k8s user is part of the cluster-admin groups.  Edit the
following command before running it; the email address should be
whatever account you use to access GKE.
* `kubectl create clusterrolebinding $USER-cluster-admin-binding
--clusterrole=cluster-admin --user=YOU@YOURDOMAIN.COM`

# Monitoring

Edit cockroachdb service to add the label prometheus:cockroachdb We use
this because we don't want to duplicate the monitoring data between two
services that we create.
* `kubectl label svc cockroachdb prometheus=cockroachdb`


Install Prometheus Operator:
* `kubectl apply -f
https://raw.githubusercontent.com/coreos/prometheus-operator/release-0.20/bundle.yaml`

Ensure that prometheus-operator has started before continuing.

Create the various objects necessary to run a prometheus instance:
* `kubectl apply -f prometheus.yaml`

To view the Prometheus UI locally:
* `kubectl port-forward
prometheus-cockroachdb-0 9090`
* Open http://localhost:9093 in your browser.
* Select the `Status -> Targets` menu entry to verify that the
  CockroachDB instances have been located.
  ![Targets screenshot](img/targets.png)
* Graphing the `sys_uptime` variable will verify that data is being
  collected. ![Uptime graph screenshot](img/graph.png)


# Alerting

Edit the template `alertmanager.yaml` with your relevant configuration.
What's in the file has a dummy web hook, per the demo.

Upload alertmanager.yaml:
* `kubectl create secret generic
alertmanager-cockroachdb --from-file=alertmanager.yaml`

It's critical that the name of the secret and the `alertmanager.yaml`
are given exactly as shown.

Create an AlertManager object to run a replicated AlertManager instance
and create a ClusterIP service so that Prometheus can forward alerts:
* `kubectl apply -f alerts.yaml`


Verify that AlertManager is running:
* `kubectl port-forward alertmanager-cockroachdb-0  9093`
* Open http://localhost:9093 in your browser.
* Ensure that the AlertManagers are visible to Prometheus by checking
  http://localhost:9090/status.  It may take a minute for the configuration
  changes to propagate.

![AlertManager screenshot](img/alertmanager.png)


Upload alert rules:
*  These are copied from https://github.com/cockroachdb/cockroach/blob/master/monitoring/rules/alerts.rules.yml:
* `kubectl apply -f alert-rules.yaml`
* Check that the rules are visible to Prometheus by opening
  http://localhost:9090/rules.  It may take a minute for the configuration
  changes to propagate. ![Rule screenshot](img/rules.png)
* Verify that the example alert is firing by opening
  http://localhost:9090/rules ![Alerts screenshot](img/alerts.png)
* Remove the example alert by running
  `kubectl edit prometheusrules prometheus-cockroachdb-rules` and
  deleting the `dummy.rules` block.
