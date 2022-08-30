// Copyright 2016 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package storage provides an easy way to work with Google Cloud Storage.
Google Cloud Storage stores data in named objects, which are grouped into buckets.

More information about Google Cloud Storage is available at
https://cloud.google.com/storage/docs.

See https://pkg.go.dev/cloud.google.com/go for authentication, timeouts,
connection pooling and similar aspects of this package.


Creating a Client

To start working with this package, create a client:

    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        // TODO: Handle error.
    }

The client will use your default application credentials. Clients should be
reused instead of created as needed. The methods of Client are safe for
concurrent use by multiple goroutines.

If you only wish to access public data, you can create
an unauthenticated client with

    client, err := storage.NewClient(ctx, option.WithoutAuthentication())

To use an emulator with this library, you can set the STORAGE_EMULATOR_HOST
environment variable to the address at which your emulator is running. This will
send requests to that address instead of to Cloud Storage. You can then create
and use a client as usual:

    // Set STORAGE_EMULATOR_HOST environment variable.
    err := os.Setenv("STORAGE_EMULATOR_HOST", "localhost:9000")
    if err != nil {
        // TODO: Handle error.
    }

    // Create client as usual.
    client, err := storage.NewClient(ctx)
    if err != nil {
        // TODO: Handle error.
    }

    // This request is now directed to http://localhost:9000/storage/v1/b
    // instead of https://storage.googleapis.com/storage/v1/b
    if err := client.Bucket("my-bucket").Create(ctx, projectID, nil); err != nil {
        // TODO: Handle error.
    }

Please note that there is no official emulator for Cloud Storage.

Buckets

A Google Cloud Storage bucket is a collection of objects. To work with a
bucket, make a bucket handle:

    bkt := client.Bucket(bucketName)

A handle is a reference to a bucket. You can have a handle even if the
bucket doesn't exist yet. To create a bucket in Google Cloud Storage,
call Create on the handle:

    if err := bkt.Create(ctx, projectID, nil); err != nil {
        // TODO: Handle error.
    }

Note that although buckets are associated with projects, bucket names are
global across all projects.

Each bucket has associated metadata, represented in this package by
BucketAttrs. The third argument to BucketHandle.Create allows you to set
the initial BucketAttrs of a bucket. To retrieve a bucket's attributes, use
Attrs:

    attrs, err := bkt.Attrs(ctx)
    if err != nil {
        // TODO: Handle error.
    }
    fmt.Printf("bucket %s, created at %s, is located in %s with storage class %s\n",
        attrs.Name, attrs.Created, attrs.Location, attrs.StorageClass)

Objects

An object holds arbitrary data as a sequence of bytes, like a file. You
refer to objects using a handle, just as with buckets, but unlike buckets
you don't explicitly create an object. Instead, the first time you write
to an object it will be created. You can use the standard Go io.Reader
and io.Writer interfaces to read and write object data:

    obj := bkt.Object("data")
    // Write something to obj.
    // w implements io.Writer.
    w := obj.NewWriter(ctx)
    // Write some text to obj. This will either create the object or overwrite whatever is there already.
    if _, err := fmt.Fprintf(w, "This object contains text.\n"); err != nil {
        // TODO: Handle error.
    }
    // Close, just like writing a file.
    if err := w.Close(); err != nil {
        // TODO: Handle error.
    }

    // Read it back.
    r, err := obj.NewReader(ctx)
    if err != nil {
        // TODO: Handle error.
    }
    defer r.Close()
    if _, err := io.Copy(os.Stdout, r); err != nil {
        // TODO: Handle error.
    }
    // Prints "This object contains text."

Objects also have attributes, which you can fetch with Attrs:

    objAttrs, err := obj.Attrs(ctx)
    if err != nil {
        // TODO: Handle error.
    }
    fmt.Printf("object %s has size %d and can be read using %s\n",
        objAttrs.Name, objAttrs.Size, objAttrs.MediaLink)

Listing objects

Listing objects in a bucket is done with the Bucket.Objects method:

    query := &storage.Query{Prefix: ""}

    var names []string
    it := bkt.Objects(ctx, query)
    for {
        attrs, err := it.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            log.Fatal(err)
        }
        names = append(names, attrs.Name)
    }

Objects are listed lexicographically by name. To filter objects
lexicographically, Query.StartOffset and/or Query.EndOffset can be used:

    query := &storage.Query{
        Prefix: "",
        StartOffset: "bar/",  // Only list objects lexicographically >= "bar/"
        EndOffset: "foo/",    // Only list objects lexicographically < "foo/"
    }

    // ... as before

If only a subset of object attributes is needed when listing, specifying this
subset using Query.SetAttrSelection may speed up the listing process:

    query := &storage.Query{Prefix: ""}
    query.SetAttrSelection([]string{"Name"})

    // ... as before

ACLs

Both objects and buckets have ACLs (Access Control Lists). An ACL is a list of
ACLRules, each of which specifies the role of a user, group or project. ACLs
are suitable for fine-grained control, but you may prefer using IAM to control
access at the project level (see
https://cloud.google.com/storage/docs/access-control/iam).

To list the ACLs of a bucket or object, obtain an ACLHandle and call its List method:

    acls, err := obj.ACL().List(ctx)
    if err != nil {
        // TODO: Handle error.
    }
    for _, rule := range acls {
        fmt.Printf("%s has role %s\n", rule.Entity, rule.Role)
    }

You can also set and delete ACLs.

Conditions

Every object has a generation and a metageneration. The generation changes
whenever the content changes, and the metageneration changes whenever the
metadata changes. Conditions let you check these values before an operation;
the operation only executes if the conditions match. You can use conditions to
prevent race conditions in read-modify-write operations.

For example, say you've read an object's metadata into objAttrs. Now
you want to write to that object, but only if its contents haven't changed
since you read it. Here is how to express that:

    w = obj.If(storage.Conditions{GenerationMatch: objAttrs.Generation}).NewWriter(ctx)
    // Proceed with writing as above.

Signed URLs

You can obtain a URL that lets anyone read or write an object for a limited time.
You don't need to create a client to do this. See the documentation of
SignedURL for details.

    url, err := storage.SignedURL(bucketName, "shared-object", opts)
    if err != nil {
        // TODO: Handle error.
    }
    fmt.Println(url)

Post Policy V4 Signed Request

A type of signed request that allows uploads through HTML forms directly to Cloud Storage with
temporary permission. Conditions can be applied to restrict how the HTML form is used and exercised
by a user.

For more information, please see https://cloud.google.com/storage/docs/xml-api/post-object as well
as the documentation of GenerateSignedPostPolicyV4.

    pv4, err := storage.GenerateSignedPostPolicyV4(bucketName, objectName, opts)
    if err != nil {
        // TODO: Handle error.
    }
    fmt.Printf("URL: %s\nFields; %v\n", pv4.URL, pv4.Fields)

Errors

Errors returned by this client are often of the type googleapi.Error.
These errors can be introspected for more information by using errors.As
with the richer googleapi.Error type. For example:

	var e *googleapi.Error
	if ok := errors.As(err, &e); ok {
		  if e.Code == 409 { ... }
	}

See https://pkg.go.dev/google.golang.org/api/googleapi#Error for more information.

Retrying failed requests

Methods in this package may retry calls that fail with transient errors.
Retrying continues indefinitely unless the controlling context is canceled, the
client is closed, or a non-transient error is received. To stop retries from
continuing, use context timeouts or cancellation.

The retry strategy in this library follows best practices for Cloud Storage. By
default, operations are retried only if they are idempotent, and exponential
backoff with jitter is employed. In addition, errors are only retried if they
are defined as transient by the service. See
https://cloud.google.com/storage/docs/retry-strategy for more information.

Users can configure non-default retry behavior for a single library call (using
BucketHandle.Retryer and ObjectHandle.Retryer) or for all calls made by a
client (using Client.SetRetry). For example:

	o := client.Bucket(bucket).Object(object).Retryer(
		// Use WithBackoff to change the timing of the exponential backoff.
		storage.WithBackoff(gax.Backoff{
			Initial:    2 * time.Second,
		}),
		// Use WithPolicy to configure the idempotency policy. RetryAlways will
		// retry the operation even if it is non-idempotent.
		storage.WithPolicy(storage.RetryAlways),
	)

	// Use a context timeout to set an overall deadline on the call, including all
	// potential retries.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Delete an object using the specified strategy and timeout.
	if err := o.Delete(ctx); err != nil {
		// Handle err.
	}
*/
package storage // import "cloud.google.com/go/storage"
