## Prometheus Helper Service

⚠️ This is an **internal** module used in the test prometheus server! ⚠️

This package starts a webserver in a specific port as mentioned in main.go. The package has 2 components that helps in handling registering and handling requests.

Note the details and purpose of this module is explained in [Dynamic Scrape Configs on Prometheus for Roachprod](https://cockroachlabs.atlassian.net/wiki/spaces/~7120207825326fb5e546c194029506f2c5335e/pages/3458531376/Dynamic+Scrape+Configs+on+Prometheus+for+Roachprod).

### Components
#### 1. Registry Handler
pkg/cmd/promhelperservice/registryhandler/registryhandler.go is responsible for creating a proxy handler that gets registered against the configured URLs. This abstracts out the http mapping of URL+Method to a handler function. The URLRegistry has the combination of URL, HTTP method and the request handler function. The list of URLRegistry is obtained from the Registry implementation.

#### 2. Service Registry
pkg/cmd/promhelperservice/handlers/registry.go implements the Registry of registryhandler.go. The implementation returns the list of URLRegistry.

### Supported URLs
`POST` `/instance-configs` - responsible for creating the prometheus instance configs with targets for port scraping.

`DELETE` `/instance-configs/{cluster_id}` - responsible for deleting the prometheus instance configs for the specified cluster.
