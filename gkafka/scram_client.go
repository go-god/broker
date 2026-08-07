package gkafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

// scramClient implements sarama.SCRAMClient.
type scramClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

// Begin initializes the SCRAM client conversation.
func (c *scramClient) Begin(userName, password, authzID string) error {
	client, err := c.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}

	c.Client = client
	c.ClientConversation = client.NewConversation()
	return nil
}

// Step processes a SCRAM challenge and returns the response.
func (c *scramClient) Step(challenge string) (string, error) {
	return c.ClientConversation.Step(challenge)
}

// Done returns whether the SCRAM conversation is complete.
func (c *scramClient) Done() bool {
	return c.ClientConversation.Done()
}

// newSCRAMClientGenerator returns a SCRAM client generator for the given mechanism.
func newSCRAMClientGenerator(mechanism string) (func() sarama.SCRAMClient, error) {
	switch mechanism {
	case sarama.SASLTypeSCRAMSHA256:
		return func() sarama.SCRAMClient {
			return &scramClient{HashGeneratorFcn: sha256.New}
		}, nil
	case sarama.SASLTypeSCRAMSHA512:
		return func() sarama.SCRAMClient {
			return &scramClient{HashGeneratorFcn: sha512.New}
		}, nil
	default:
		return nil, fmt.Errorf("unsupported sasl mechanism: %s", mechanism)
	}
}
