// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// sshKeyExists checks to see if there is a an SSH key with the given name in the given region.
func (p *Provider) sshKeyExists(l *logger.Logger, keyName, region string) (bool, error) {
	var data struct {
		KeyPairs []struct {
			KeyName string
		}
	}
	args := []string{
		"ec2", "describe-key-pairs",
		"--region", region,
	}
	err := p.runJSONCommand(l, args, &data)
	if err != nil {
		return false, err
	}
	for _, keyPair := range data.KeyPairs {
		if keyPair.KeyName == keyName {
			return true, nil
		}
	}
	return false, nil
}

// sshKeyImport takes the user's local, public SSH key and imports it into the ec2 region so that
// we can create new hosts with it.
func (p *Provider) sshKeyImport(l *logger.Logger, keyName, region string) error {
	sshPublicKeyPath, err := config.SSHPublicKeyPath()
	if err != nil {
		return err
	}

	var data struct {
		KeyName string
	}
	_ = data.KeyName // silence unused warning

	user, err := p.FindActiveAccount(l)
	if err != nil {
		return err
	}

	timestamp := timeutil.Now()
	createdAt := timestamp.Format(time.RFC3339)

	IAMUserNameTag := fmt.Sprintf("{Key=IAMUserName,Value=%s}", user)
	createdAtTag := fmt.Sprintf("{Key=CreatedAt,Value=%s}", createdAt)
	tagSpecs := fmt.Sprintf("ResourceType=key-pair,Tags=[%s, %s]", IAMUserNameTag, createdAtTag)

	args := []string{
		"ec2", "import-key-pair",
		"--region", region,
		"--key-name", keyName,
		"--public-key-material", fmt.Sprintf("fileb://%s", sshPublicKeyPath),
		"--tag-specifications", tagSpecs,
	}
	err = p.runJSONCommand(l, args, &data)
	// If two roachprod instances run at the same time with the same key, they may
	// race to upload the key pair.
	if err == nil || strings.Contains(err.Error(), "InvalidKeyPair.Duplicate") {
		return nil
	}
	return err
}

// sshKeyName computes the name of the ec2 ssh key that we'll store the local user's public key in
func (p *Provider) sshKeyName(l *logger.Logger) (string, error) {
	user, err := p.FindActiveAccount(l)
	if err != nil {
		return "", err
	}

	sshKey, err := config.SSHPublicKey()
	if err != nil {
		return "", err
	}

	hash := sha1.New()
	if _, err := hash.Write([]byte(sshKey)); err != nil {
		return "", err
	}
	hashBytes := hash.Sum(nil)
	hashText := base64.URLEncoding.EncodeToString(hashBytes)

	return fmt.Sprintf("%s-%s", user, hashText), nil
}
