#!/bin/bash

# Copyright 2025 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

# Exit immediately if a command exits with a non-zero status
set -e

# Set global variables
PROJECT_ID="cockroach-drt"
GKE_CLUSTER_NAME="crdb"
REGION="us-east1"
MACHINE_TYPE="n2-standard-4"
USER_EMAIL=$(gcloud config get-value account)
DATADOG_API_KEY="$(gcloud --project=cockroach-drt secrets versions access latest --secret datadog-api-key)"

# cluster configuration
export CLUSTER_NAME="${CLUSTER_NAME:-drt-k8s}"
export NODE_COUNT="${NODE_COUNT:-3}"
export STORAGE_SIZE="${STORAGE_SIZE:-60Gi}"
export CPU_REQUEST="${CPU_REQUEST:-500m}"
export CPU_LIMIT="${CPU_LIMIT:-2}"
export MEMORY_REQUEST="${MEMORY_REQUEST:-2Gi}"
export MEMORY_LIMIT="${MEMORY_LIMIT:-8Gi}"
export COCKROACH_VERSION="${COCKROACH_VERSION:-v24.2.5}"


# Trap errors to provide more context
trap 'echo "Error: Command failed on line $LINENO"; exit 1' ERR

# Logging function
log() {
    echo "*********************************"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*"
    echo "*********************************"
}

# Function to check if cluster exists
cluster_exists() {
    gcloud container clusters describe "$GKE_CLUSTER_NAME" --region "$REGION" --project "$PROJECT_ID" &> /dev/null
}

# Function to create GKE cluster
create_gke_cluster() {
    local total_nodes=$1
    export NODE_COUNT="${total_nodes}"


    if cluster_exists; then
        log "Cluster $GKE_CLUSTER_NAME already exists. Skipping creation."
        return 0
    fi

    # Validate total nodes is a multiple of 3
    if ((total_nodes % 3 != 0)); then
        log "Error: Total nodes must be a multiple of 3 (e.g., 3, 6, 9, 12)"
        return 1
    fi

    local nodes_per_zone=$((total_nodes / 3))

    log "Creating GKE cluster with $total_nodes nodes ($nodes_per_zone per zone)..."

    gcloud container clusters create "$GKE_CLUSTER_NAME" \
        --machine-type "$MACHINE_TYPE" \
        --region "$REGION" \
        --num-nodes "$nodes_per_zone" \
        --project="$PROJECT_ID"

    # Create cluster role binding (idempotent)
    kubectl create clusterrolebinding "$USER-cluster-admin-binding" \
        --clusterrole=cluster-admin \
        --user="$USER_EMAIL" \
        --dry-run=client -o yaml | kubectl apply -f -
}

# Function to setup GKE credentials
setup_gke() {
    log "Setting up GKE credentials..."

    # Fetch cluster credentials
    gcloud container clusters get-credentials "$GKE_CLUSTER_NAME" --region "$REGION" --project "$PROJECT_ID"
}

# Function to deploy CockroachDB
deploy_cockroachdb() {
    log "Deploying CockroachDB operator and crdb cluster..."

    # Create namespace first
    kubectl create namespace cockroach-operator-system --dry-run=client -o yaml | kubectl apply -f -

    # Deploy CRDs (use apply to make it idempotent)
    kubectl apply -f https://raw.githubusercontent.com/cockroachdb/cockroach-operator/v2.16.1/install/crds.yaml

    # Deploy Operator
    kubectl apply -f https://raw.githubusercontent.com/cockroachdb/cockroach-operator/v2.16.1/install/operator.yaml
    # wait for deployment
    kubectl rollout status deployment cockroach-operator-manager -n cockroach-operator-system

    # Generate configurable YAML
    envsubst < cockroachdb-template.yaml > cockroachdb-cluster.yaml
    # Apply generated YAML
    kubectl apply -f cockroachdb-cluster.yaml --namespace=cockroach-operator-system
    kubectl wait crdbcluster ${CLUSTER_NAME} \
        --for=jsonpath='{.status.clusterStatus}'=Finished \
        -n cockroach-operator-system \
        --timeout=10m


    # Optional: Deploy SQL client
    envsubst < sql-client-template.yaml > sql-client.yaml
    kubectl apply -f sql-client.yaml --namespace=cockroach-operator-system
    kubectl rollout status deployment cockroachdb-client-secure -n cockroach-operator-system

    sleep 60

    #create user roachprod
    create_user "roachprod" "cockroachdb"
    grant_role "roachprod" "admin"

    #set enterprise license
    execute_sql "SET CLUSTER SETTING enterprise.license = '${COCKROACH_DEV_LICENSE}';"

}

execute_cmd() {
  local cmd=$1
  if [ -z "$cmd" ]; then
    log "command cannot be empty"
    return 1
  fi

  podname=$(kubectl get pods -l app=cockroachdb-client-secure -o jsonpath='{.items[*].metadata.name}' -n cockroach-operator-system)
  if [ -z "$podname" ]; then
      log "no sql client running"
      return 1
  fi

  kubectl exec --namespace=cockroach-operator-system -it ${podname} -- $cmd
}

# execute a sql command
execute_sql() {
  local sql_statement=$1
  if [ -z "$sql_statement" ]; then
    log "sql statement cannot be empty"
    return 1
  fi

  podname=$(kubectl get pods -l app=cockroachdb-client-secure -o jsonpath='{.items[*].metadata.name}' -n cockroach-operator-system)
  if [ -z "$podname" ]; then
      log "no sql client running"
      return 1
  fi

  kubectl exec --namespace=cockroach-operator-system -it ${podname} -- ./cockroach sql --certs-dir=/cockroach/certs --host=drt-k8s-public --execute="${sql_statement}" || true
}

# create a sql user
create_user() {
  local username=$1
  local password=$2

  execute_sql "CREATE USER ${username} WITH PASSWORD '${password}';"
}

# grant role to sql user
grant_role() {
  local username=$1
  local role=$2

  execute_sql "GRANT ${role} TO ${username};"
}

# adminurl to access db console
admin_url() {
  log "http://localhost:8080"
  kubectl port-forward -n cockroach-operator-system service/${CLUSTER_NAME}-public 8080
}

# pgurl
pg_url() {
  echo "postgresql://root@${CLUSTER_NAME}-public.cockroach-operator-system.svc.cluster.local:26257/?sslmode=require&sslrootcert=/cockroach/certs/ca.crt&sslkey=/cockroach/certs/client.root.key&sslcert=/cockroach/certs/client.root.crt"
}

# Function to deploy Datadog monitoring
deploy_datadog() {
    log "Setting up Datadog monitoring..."

    # Add repo and update
    helm repo add datadog https://helm.datadoghq.com
    helm repo update

    # Create secret (use kubectl create if not exists)
    kubectl create secret generic datadog-secret \
        --from-literal=api-key="$DATADOG_API_KEY" \
        --dry-run=client -o yaml | kubectl apply -f -

    envsubst < datadog-values-template.yaml > datadog.yaml

    # Install Datadog (use helm upgrade --install for idempotency)
    helm upgrade --install datadog-agent \
        -f datadog.yaml \
        datadog/datadog
}

# Function to destroy all Kubernetes objects
destroy_objects() {
    log "Destroying Kubernetes objects..."

    # Use || true to continue even if deletion fails
    envsubst < cockroachdb-template.yaml > cockroachdb-cluster.yaml
    kubectl delete -f cockroachdb-cluster.yaml --namespace=cockroach-operator-system || true

    envsubst < sql-client-template.yaml > sql-client.yaml
    kubectl delete -f sql-client.yaml --namespace=cockroach-operator-system || true

    kubectl delete -f https://raw.githubusercontent.com/cockroachdb/cockroach-operator/v2.16.1/install/operator.yaml || true
    kubectl delete pvc --all -n cockroach-operator-system || true

    helm uninstall datadog-agent || true
    kubectl delete secret datadog-secret || true
}

# Function to destroy GKE cluster
destroy_gke() {
    if ! cluster_exists; then
        log "Cluster $GKE_CLUSTER_NAME does not exist. Nothing to destroy."
        return 0
    fi

    log "Destroying GKE cluster..."
    gcloud container clusters delete "$GKE_CLUSTER_NAME" --region "$REGION" --project "$PROJECT_ID" --quiet
}

check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo "Error: $1 is not installed."
        case "$1" in
            gcloud)
                echo "Install Google Cloud SDK: https://cloud.google.com/sdk/docs/install"
                ;;
            kubectl)
                echo "Install kubectl:
- macOS/Linux: 'curl -LO \"https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl\"'
- Windows: 'choco install kubernetes-cli'"
                ;;
            helm)
                echo "Install Helm:
- macOS: 'brew install helm'
- Linux: 'curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash'"
                ;;
        esac
        return 1
    fi
    return 0
}

# Pre-flight checks
preflight_checks() {
    local commands=("gcloud" "kubectl" "helm")
    local missing=0

    for cmd in "${commands[@]}"; do
        if ! check_command "$cmd"; then
            ((missing++))
        fi
    done

    if ((missing > 0)); then
        echo "Please install the missing dependencies before proceeding."
        exit 1
    fi

    # Additional checks
    if ! gcloud config get-value account &> /dev/null; then
        echo "Error: You are not authenticated with gcloud. Run 'gcloud auth login'."
        exit 1
    fi
}


# Main script logic
main() {
    preflight_checks

    case "$1" in
        create)
            create_gke_cluster "$2"
            deploy_cockroachdb
            deploy_datadog
            ;;
        setup-gke)
            setup_gke
            ;;
        destroy)
            destroy_objects
            ;;
        destroy-gke)
            destroy_gke
            ;;
        adminurl)
            admin_url
            ;;
        pgurl)
            pg_url
            ;;
        sql)
            execute_sql "$2"
            ;;
        run)
            execute_cmd "$2"
            ;;
        *)
            echo "Usage: $0 {create <total_nodes>|setup-gke|destroy|destroy-gke|adminurl|pgurl|sql <sql_statement>}|run <cmd>"
            echo "*************************"
            echo "To run workload command see example:"
            echo "Import ./cockroach workload fixtures import tpcc <pgurl> --warehouses=500 --db=cct_tpcc --checks=false"
            echo "./cluster_manager.sh run \"./cockroach workload run tpcc --workers=10 --conns=10 --ramp=10m --wait=0 --db=cct_tpcc --warehouses=500 --max-rate=100 --tolerate-errors  --families <pg_url>\""
            exit 1
    esac
}

# Run main function with all arguments
main "$@"
