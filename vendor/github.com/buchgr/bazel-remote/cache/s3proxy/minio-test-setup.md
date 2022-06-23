# Testing the S3 proxy backend with MinIO

Note: this test setup uses default username/password, and is not suitable for
production use.

## Download the MinIO server and client

    wget https://dl.min.io/server/minio/release/linux-amd64/minio
    wget https://dl.min.io/client/mc/release/linux-amd64/mc
    chmod +x minio mc

## Start the MinIO server and create a bucket

    # Start a server with the cache stored in "miniocachedir":
    ./minio server miniocachedir &

    # Add a new host alias(?) with the default username/password:
    ./mc config host add myminio http://127.0.0.1:9000 minioadmin minioadmin

    # Create a "bazel-remote" bucket:
    ./mc mb myminio/bazel-remote

## Run bazel-remote with the S3 proxy backend

    ./bazel-remote --dir bazel-remote-cachedir --max_size 5 \
    	--s3.endpoint 127.0.0.1:9000 \
    	--s3.bucket bazel-remote \
    	--s3.prefix files \
    	--s3.access_key_id minioadmin \
    	--s3.secret_access_key minioadmin \
    	--s3.disable_ssl

## Build something with the bazel-remote cache

    bazel build //:bazel-remote --remote_cache=grpc://127.0.0.1:9092

Now you can login to the MinIO web interface at http://127.0.0.1:9000 with
username/password minioadmin/minioadmin and inspect the bucket and uploaded
blobs.
