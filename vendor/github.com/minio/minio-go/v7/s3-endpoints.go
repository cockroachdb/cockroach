/*
 * MinIO Go Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2015-2017 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package minio

// awsS3EndpointMap Amazon S3 endpoint map.
var awsS3EndpointMap = map[string]string{
	"us-east-1":      "s3.dualstack.us-east-1.amazonaws.com",
	"us-east-2":      "s3.dualstack.us-east-2.amazonaws.com",
	"us-west-2":      "s3.dualstack.us-west-2.amazonaws.com",
	"us-west-1":      "s3.dualstack.us-west-1.amazonaws.com",
	"ca-central-1":   "s3.dualstack.ca-central-1.amazonaws.com",
	"eu-west-1":      "s3.dualstack.eu-west-1.amazonaws.com",
	"eu-west-2":      "s3.dualstack.eu-west-2.amazonaws.com",
	"eu-west-3":      "s3.dualstack.eu-west-3.amazonaws.com",
	"eu-central-1":   "s3.dualstack.eu-central-1.amazonaws.com",
	"eu-north-1":     "s3.dualstack.eu-north-1.amazonaws.com",
	"eu-south-1":     "s3.dualstack.eu-south-1.amazonaws.com",
	"ap-east-1":      "s3.dualstack.ap-east-1.amazonaws.com",
	"ap-south-1":     "s3.dualstack.ap-south-1.amazonaws.com",
	"ap-southeast-1": "s3.dualstack.ap-southeast-1.amazonaws.com",
	"ap-southeast-2": "s3.dualstack.ap-southeast-2.amazonaws.com",
	"ap-northeast-1": "s3.dualstack.ap-northeast-1.amazonaws.com",
	"ap-northeast-2": "s3.dualstack.ap-northeast-2.amazonaws.com",
	"ap-northeast-3": "s3.dualstack.ap-northeast-3.amazonaws.com",
	"af-south-1":     "s3.dualstack.af-south-1.amazonaws.com",
	"me-south-1":     "s3.dualstack.me-south-1.amazonaws.com",
	"sa-east-1":      "s3.dualstack.sa-east-1.amazonaws.com",
	"us-gov-west-1":  "s3.dualstack.us-gov-west-1.amazonaws.com",
	"us-gov-east-1":  "s3.dualstack.us-gov-east-1.amazonaws.com",
	"cn-north-1":     "s3.dualstack.cn-north-1.amazonaws.com.cn",
	"cn-northwest-1": "s3.dualstack.cn-northwest-1.amazonaws.com.cn",
}

// getS3Endpoint get Amazon S3 endpoint based on the bucket location.
func getS3Endpoint(bucketLocation string) (s3Endpoint string) {
	s3Endpoint, ok := awsS3EndpointMap[bucketLocation]
	if !ok {
		// Default to 's3.dualstack.us-east-1.amazonaws.com' endpoint.
		s3Endpoint = "s3.dualstack.us-east-1.amazonaws.com"
	}
	return s3Endpoint
}
