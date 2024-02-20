# The intention is that you'll execute the script at the end of your Bazel
# invocation as follows: `bazel test ... $(engflow-args.sh)`. This will add
# remote execution arguments to the invocation. You must call get-engflow-keys.sh
# before this.

echo '--config engflowpublic --tls_client_certificate=/home/agent/engflow.crt --tls_client_key=/home/agent/engflow.key'
