// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cloud

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

func getEC2Client(region string) (*ec2.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, errors.Wrap(err, "getEC2Client: failed to get EC2 client")
	}
	return ec2.NewFromConfig(cfg), nil
}

func getIAMClient(region string) (*iam.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, errors.Wrap(err, "getIAMClient: failed to get IAM client")
	}
	return iam.NewFromConfig(cfg), nil
}

func getIAMUsers(IAMClient *iam.Client) ([]iamtypes.User, error) {
	var users []iamtypes.User
	input := iam.ListUsersInput{}

	// isTruncated indicates whether there are more users to return.
	isTruncated := true
	for isTruncated {
		resp, err := IAMClient.ListUsers(context.TODO(), &input)
		if err != nil {
			return nil, errors.Wrap(err, "getIAMUsers: failed to list IAM users")
		}
		users = append(users, resp.Users...)

		isTruncated = resp.IsTruncated
		if isTruncated {
			// Marker indicates where the next call should start incase of paginated results.
			input = iam.ListUsersInput{Marker: resp.Marker}
		}
	}
	return users, nil
}

func getUsersWithActiveAccessKey(
	IAMClient *iam.Client, users []iamtypes.User,
) (map[string]bool, error) {
	usersWithActiveAccessKey := make(map[string]bool)
	for _, user := range users {
		input := iam.ListAccessKeysInput{UserName: user.UserName}
		// isTruncated indicates whether there are more keys to return.
		isTruncated := true
	outerLoop:
		for isTruncated {
			resp, err := IAMClient.ListAccessKeys(context.TODO(), &input)
			if err != nil {
				return nil, errors.Wrap(err, "getUsersWithActiveAccessKey: failed to list access keys")
			}
			for _, key := range resp.AccessKeyMetadata {
				if key.Status == "Active" {
					usersWithActiveAccessKey[*user.UserName] = true
					break outerLoop
				}
			}

			isTruncated = resp.IsTruncated
			if isTruncated {
				// Marker indicates where the next call should start incase of paginated results.
				input = iam.ListAccessKeysInput{UserName: user.UserName, Marker: resp.Marker}
			}
		}
	}
	return usersWithActiveAccessKey, nil
}

func getUsersWithConsoleAccess(
	IAMClient *iam.Client, users []iamtypes.User,
) (map[string]bool, error) {
	usersWithConsoleAccess := make(map[string]bool)
	for _, user := range users {
		input := iam.GetLoginProfileInput{UserName: user.UserName}
		_, err := IAMClient.GetLoginProfile(context.TODO(), &input)
		if err != nil {
			// NoSuchEntityException means user doesn't have console access.
			var nse *iamtypes.NoSuchEntityException
			if errors.As(err, &nse) {
				continue
			}
			return nil, errors.Wrap(err, "getUsersWithConsoleAccess: failed to get login profile")
		}
		usersWithConsoleAccess[*user.UserName] = true
	}
	return usersWithConsoleAccess, nil
}

func getUsersWithMFAEnabled(IAMClient *iam.Client, users []iamtypes.User) (map[string]bool, error) {
	usersWithMFAEnabled := make(map[string]bool)
	for _, user := range users {
		input := iam.ListMFADevicesInput{UserName: user.UserName}
		resp, err := IAMClient.ListMFADevices(context.TODO(), &input)
		if err != nil {
			return nil, errors.Wrap(err, "getUsersWithMFAEnabled: failed to list mfa devices")
		}
		if len(resp.MFADevices) > 0 {
			usersWithMFAEnabled[*user.UserName] = true
		}
	}
	return usersWithMFAEnabled, nil
}

func getRegions() ([]ec2types.Region, error) {
	// Pass empty string as region to use default region (no preferred region for this call).
	EC2Client, err := getEC2Client("")
	if err != nil {
		return nil, err
	}
	input := ec2.DescribeRegionsInput{}
	// DescribeRegions returns all regions enabled for the AWS account.
	resp, err := EC2Client.DescribeRegions(context.TODO(), &input)
	if err != nil {
		return nil, errors.Wrap(err, "getRegions: failed to describe regions")
	}
	return resp.Regions, nil
}

func getTagsValues(tags []ec2types.Tag) (string, string) {
	IAMUserName := ""
	createdAt := ""
	for _, tag := range tags {
		if *tag.Key == "IAMUserName" {
			IAMUserName = *tag.Value
		} else if *tag.Key == "CreatedAt" {
			createdAt = *tag.Value
		}
	}
	return IAMUserName, createdAt
}

// Returns IAMUserName if key was created by roachprod, otherwise returns empty string.
// Expected keypair name format: ${IAMUserName}-${PublicKeyHash}
// ${PublicKeyHash} will always be 28 characters (SHA1 hash encoded into base64).
func getIAMUserNameFromKeyname(keyName string) string {
	if len(keyName) > 29 && keyName[len(keyName)-29:len(keyName)-29+1] == "-" {
		return keyName[:len(keyName)-29]
	}
	return ""
}

func getKeyPairs(EC2Client *ec2.Client) ([]ec2types.KeyPairInfo, error) {
	input := ec2.DescribeKeyPairsInput{}
	resp, err := EC2Client.DescribeKeyPairs(context.TODO(), &input)
	if err != nil {
		return nil, errors.Wrap(err, "getKeyPairs: failed to describe key pairs")
	}
	return resp.KeyPairs, nil
}

// Tag keypair with IAMUserName and CreatedAt if untagged and return createdAtValue.
func tagKeyPairIfUntagged(
	EC2Client *ec2.Client,
	keyPair ec2types.KeyPairInfo,
	IAMUserName string,
	timestamp time.Time,
	dryrun bool,
) (string, error) {
	IAMUserNameTag, createdAtTag := getTagsValues(keyPair.Tags)
	var tags []ec2types.Tag
	if IAMUserNameTag == "" {
		IAMUserNameKey := "IAMUserName"
		tags = append(tags, ec2types.Tag{Key: &IAMUserNameKey, Value: &IAMUserName})
		log.Infof(context.Background(), "Tagging %s with IAMUserName: %s\n", *keyPair.KeyName, IAMUserName)
	}
	createdAtValue := timestamp.Format(time.RFC3339)
	if createdAtTag == "" {
		createdAtKey := "CreatedAt"
		tags = append(tags, ec2types.Tag{Key: &createdAtKey, Value: &createdAtValue})
		log.Infof(context.Background(), "Tagging %s with CreatedAt: %s\n", *keyPair.KeyName, createdAtValue)
	} else {
		createdAtValue = createdAtTag
	}

	if !dryrun && len(tags) > 0 {
		input := ec2.CreateTagsInput{Resources: []string{*keyPair.KeyPairId}, Tags: tags}
		_, err := EC2Client.CreateTags(context.TODO(), &input)
		if err != nil {
			return "", errors.Wrapf(err, "tagKeyPair: failed to create tags for key: %s", *keyPair.KeyName)
		}
	}
	return createdAtValue, nil
}

func deleteKeyPair(EC2Client *ec2.Client, keyPairName string) error {
	input := ec2.DeleteKeyPairInput{KeyName: &keyPairName}
	_, err := EC2Client.DeleteKeyPair(context.TODO(), &input)
	if err != nil {
		return errors.Wrap(err, "deleteKeyPair: failed to delete key pair")
	}
	return nil
}

// GCAWSKeyPairs tags keypairs created by roachprod with IAMUserName and CreatedAt if untagged and
// deletes keypairs created by previous users/employees (TeamCity keypairs are deleted after 10 days).
func GCAWSKeyPairs(dryrun bool) error {
	timestamp := timeutil.Now()

	// Pass empty string as region to use default region (IAM is global).
	IAMClient, err := getIAMClient("")
	if err != nil {
		return err
	}

	IAMUsers, err := getIAMUsers(IAMClient)
	if err != nil {
		return err
	}
	usersWithActiveAccessKey, err := getUsersWithActiveAccessKey(IAMClient, IAMUsers)
	if err != nil {
		return err
	}
	usersWithMFAEnabled, err := getUsersWithMFAEnabled(IAMClient, IAMUsers)
	if err != nil {
		return err
	}
	usersWithConsoleAccess, err := getUsersWithConsoleAccess(IAMClient, IAMUsers)
	if err != nil {
		return err
	}
	regions, err := getRegions()
	if err != nil {
		return err
	}
	for _, region := range regions {
		log.Infof(context.Background(), "%s", *region.RegionName)
		EC2Client, err := getEC2Client(*region.RegionName)
		if err != nil {
			return err
		}
		keyPairs, err := getKeyPairs(EC2Client)
		if err != nil {
			return err
		}
		for _, keyPair := range keyPairs {

			IAMUserName := getIAMUserNameFromKeyname(*keyPair.KeyName)
			if IAMUserName == "" {
				// keypair wasn't created by roachprod
				continue
			}
			createdAt, err := tagKeyPairIfUntagged(EC2Client, keyPair, IAMUserName, timestamp, dryrun)
			if err != nil {
				return err
			}

			// teamcity-runner keys should only be deleted 10 days after creation
			if IAMUserName == "teamcity-runner" {
				createdAtTimestamp, err := time.Parse(time.RFC3339, createdAt)
				if err != nil {
					return err
				}
				// 10 days = 240 hours
				if timestamp.Sub(createdAtTimestamp).Hours() >= 240 {
					log.Infof(context.Background(), "Deleting %s because it is a teamcity-runner key created at %s.\n",
						*keyPair.KeyName, createdAtTimestamp)
					if !dryrun {
						err := deleteKeyPair(EC2Client, *keyPair.KeyName)
						if err != nil {
							return err
						}
					}
				}
				continue
			}

			// Delete key if user has console access without MFA".
			if usersWithConsoleAccess[IAMUserName] && !usersWithMFAEnabled[IAMUserName] {
				log.Infof(context.Background(), "Deleting %s because %s has console access but MFA disabled.\n", *keyPair.KeyName, IAMUserName)
				if !dryrun {
					err := deleteKeyPair(EC2Client, *keyPair.KeyName)
					if err != nil {
						return err
					}
				}
				// Delete key if user doesn't have an active access key.
			} else if !usersWithActiveAccessKey[IAMUserName] {
				log.Infof(context.Background(), "Deleting %s because %s does not have an active access key.\n",
					*keyPair.KeyName, IAMUserName)
				if !dryrun {
					err := deleteKeyPair(EC2Client, *keyPair.KeyName)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
