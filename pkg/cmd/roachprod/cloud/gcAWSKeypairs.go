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
	// "fmt"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	iamtypes "github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func getEC2Client(region string) (*ec2.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return ec2.NewFromConfig(cfg), nil
}

func getIAMClient(region string) (*iam.Client, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, err
	}
	return iam.NewFromConfig(cfg), nil
}

func getIAMUsers() ([]iamtypes.User, error) {
	IAMClient, err := getIAMClient("")
	if err != nil {
		return nil, err
	}
	// MaxItems (ie: max returned users) default value is less than the number of users we have.
	maxitems := int32(1000)
	input := iam.ListUsersInput{MaxItems: &maxitems}
	resp, err := IAMClient.ListUsers(context.TODO(), &input)
	if err != nil {
		return nil, err
	}
	return resp.Users, nil
}

func getUsersWithActiveAccessKey(users []iamtypes.User) (map[string]bool, error) {
	IAMClient, err := getIAMClient("")
	if err != nil {
		return nil, err
	}
	userHasAccessKey := make(map[string]bool)
	for _, user := range users {
		input := iam.ListAccessKeysInput{UserName: user.UserName}
		resp, err := IAMClient.ListAccessKeys(context.TODO(), &input)
		if err != nil {
			return nil, err
		}
		accessKeys := resp.AccessKeyMetadata
		for _, key := range accessKeys {
			if key.Status == "Active" {
				userHasAccessKey[*user.UserName] = true
				break
			}
		}
	}
	return userHasAccessKey, nil
}

func getUsersWithConsoleAccess(users []iamtypes.User) (map[string]bool, error) {
	IAMClient, err := getIAMClient("")
	if err != nil {
		return nil, err
	}
	userHasConsoleAccess := make(map[string]bool)
	for _, user := range users {
		input := iam.GetLoginProfileInput{UserName: user.UserName}
		_, err := IAMClient.GetLoginProfile(context.TODO(), &input)
		if err != nil {
			// NoSuchEntityException means user doesn't have console access.
			var nse *iamtypes.NoSuchEntityException
			if errors.As(err, &nse) {
				continue
			}
			return nil, err
		}
		userHasConsoleAccess[*user.UserName] = true
	}
	return userHasConsoleAccess, nil
}

func getUsersWithMFAEnabled(users []iamtypes.User) (map[string]bool, error) {
	IAMClient, err := getIAMClient("")
	if err != nil {
		return nil, err
	}
	userHasMFAEnabled := make(map[string]bool)
	for _, user := range users {
		input := iam.ListMFADevicesInput{UserName: user.UserName}
		resp, err := IAMClient.ListMFADevices(context.TODO(), &input)
		if err != nil {
			return nil, err
		}
		if len(resp.MFADevices) > 0 {
			userHasMFAEnabled[*user.UserName] = true
		}
	}
	return userHasMFAEnabled, nil
}

func getRegions() ([]ec2types.Region, error) {
	EC2Client, err := getEC2Client("")
	if err != nil {
		return nil, err
	}
	input := ec2.DescribeRegionsInput{}
	// DescribeRegions returns all regions enabled for the AWS account.
	resp, err := EC2Client.DescribeRegions(context.TODO(), &input)
	if err != nil {
		return nil, err
	}
	return resp.Regions, nil
}

func getTagsValues(tags []ec2types.Tag) (string, string) {
	IAMUserName := ""
	CreatedAt := ""
	for _, tag := range tags {
		if *tag.Key == "IAMUserName" {
			IAMUserName = *tag.Value
		} else if *tag.Key == "CreatedAt" {
			CreatedAt = *tag.Value
		}
	}
	return IAMUserName, CreatedAt
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

func getKeyPairs(region string) ([]ec2types.KeyPairInfo, error) {
	EC2Client, err := getEC2Client(region)
	if err != nil {
		return nil, err
	}
	input := ec2.DescribeKeyPairsInput{}
	resp, err := EC2Client.DescribeKeyPairs(context.TODO(), &input)
	if err != nil {
		return nil, err
	}
	return resp.KeyPairs, nil
}

// Tag keypair with IAMUserName and CreatedAt if untagged and return CreatedAtValue.
func tagKeyPair(
	region string, keyPair ec2types.KeyPairInfo, IAMUserName string, creationTimestamp time.Time,
) (string, error) {
	IAMUserNameTag, CreatedAtTag := getTagsValues(keyPair.Tags)
	var tags []ec2types.Tag
	if IAMUserNameTag == "" {
		IAMUserNameKey := "IAMUserName"
		tags = append(tags, ec2types.Tag{Key: &IAMUserNameKey, Value: &IAMUserName})
	}
	CreatedAtValue := creationTimestamp.Format(time.RFC3339)
	if CreatedAtTag == "" {
		CreatedAtKey := "CreatedAt"
		tags = append(tags, ec2types.Tag{Key: &CreatedAtKey, Value: &CreatedAtValue})
	} else {
		CreatedAtValue = CreatedAtTag
	}
	if len(tags) == 0 {
		return CreatedAtValue, nil
	}
	EC2Client, err := getEC2Client(region)
	if err != nil {
		return "", err
	}
	input := ec2.CreateTagsInput{Resources: []string{*keyPair.KeyPairId}, Tags: tags}
	_, err = EC2Client.CreateTags(context.TODO(), &input)
	if err != nil {
		return "", err
	}
	return CreatedAtValue, nil
}

func deleteKeyPair(region string, keyPairName string) error {
	EC2Client, err := getEC2Client(region)
	if err != nil {
		return err
	}
	input := ec2.DeleteKeyPairInput{KeyName: &keyPairName}
	_, err = EC2Client.DeleteKeyPair(context.TODO(), &input)
	if err != nil {
		return err
	}
	return nil
}

// GCAWSKeyPairs tags keypairs created by roachprod with IAMUserName and CreatedAt if untagged and
// deletes keypairs created by previous users/employees (TeamCity keypairs are deleted after 10 days).
func GCAWSKeyPairs() error {
	timestamp := timeutil.Now()
	IAMUsers, err := getIAMUsers()
	if err != nil {
		return err
	}
	hasActiveAccessKey, err := getUsersWithActiveAccessKey(IAMUsers)
	if err != nil {
		return err
	}
	hasMFAEnabled, err := getUsersWithMFAEnabled(IAMUsers)
	if err != nil {
		return err
	}
	hasConsoleAccess, err := getUsersWithConsoleAccess(IAMUsers)
	if err != nil {
		return err
	}
	regions, err := getRegions()
	if err != nil {
		return err
	}
	for _, region := range regions {
		keyPairs, err := getKeyPairs(*region.RegionName)
		if err != nil {
			return err
		}
		for _, keyPair := range keyPairs {

			IAMUserName := getIAMUserNameFromKeyname(*keyPair.KeyName)
			if IAMUserName == "" {
				// keypair wasn't created by roachprod
				continue
			}
			CreatedAt, err := tagKeyPair(*region.RegionName, keyPair, IAMUserName, timestamp)
			if err != nil {
				return err
			}

			if IAMUserName == "teamcity-runner" {
				CreatedAtTimestamp, err := time.Parse(time.RFC3339, CreatedAt)
				if err != nil {
					return err
				}
				if timestamp.Sub(CreatedAtTimestamp).Hours() >= 240 {
					err := deleteKeyPair(*region.RegionName, *keyPair.KeyName)
					if err != nil {
						return err
					}
					// fmt.Printf("Deleting %s\n", IAMUserName)
				}
				continue
			}

			if hasConsoleAccess[IAMUserName] && !hasMFAEnabled[IAMUserName] {
				err := deleteKeyPair(*region.RegionName, *keyPair.KeyName)
				if err != nil {
					return err
				}
				// fmt.Printf("Deleting %s\n", *keyPair.KeyName)
			} else if !hasActiveAccessKey[IAMUserName] {
				err := deleteKeyPair(*region.RegionName, *keyPair.KeyName)
				if err != nil {
					return err
				}
				// fmt.Printf("Deleting %s\n", *keyPair.KeyName)
			}
		}
	}
	return nil
}
