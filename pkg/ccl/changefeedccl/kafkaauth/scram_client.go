// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kafkaauth

import (
	"crypto/sha256"
	"crypto/sha512"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

var (
	// sha256ClientGenerator returns a SCRAMClient for the
	// SCRAM-SHA-256 SASL mechanism. This can used as a
	// SCRAMCLientGeneratorFunc when constructing a sarama SASL
	// configuration.
	sha256ClientGenerator = func() sarama.SCRAMClient {
		return &scramClient{HashGeneratorFcn: sha256.New}
	}

	// sha512ClientGenerator returns a SCRAMClient for the
	// SCRAM-SHA-512 SASL mechanism. This can used as a
	// SCRAMCLientGeneratorFunc when constructing a sarama SASL
	// configuration.
	sha512ClientGenerator = func() sarama.SCRAMClient {
		return &scramClient{HashGeneratorFcn: sha512.New}
	}
)

type scramClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

var _ sarama.SCRAMClient = &scramClient{}

func (c *scramClient) Begin(userName, password, authzID string) error {
	var err error
	c.Client, err = c.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	c.ClientConversation = c.Client.NewConversation()
	return nil
}

func (c *scramClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

func (c *scramClient) Done() bool {
	return c.ClientConversation.Done()
}
