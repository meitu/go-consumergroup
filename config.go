package consumergroup

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
)

// Config is used to pass configuration options to consumer groups. Items can
// be modified directly.
type Config struct {
	groupID   string
	topicList []string

	SaramaConfig *sarama.Config

	ErrorChannelBufferSize int

	OffsetAutoCommitInterval time.Duration
	OffsetAutoReset          int64

	ClaimPartitionRetry         int
	ClaimPartitionRetryInterval time.Duration
}

// NewConfig creates a new Config instance.
func NewConfig(groupID string, topicList []string) (*Config, error) {
	if groupID == "" {
		return nil, errors.New("group id is invalid")
	}

	if len(topicList) == 0 {
		return nil, errors.New("topic list is invalid")
	}
	topicList = SliceRemoveDuplicates(topicList)

	config := new(Config)
	config.groupID = groupID
	config.topicList = topicList

	config.SaramaConfig = sarama.NewConfig()

	config.ErrorChannelBufferSize = 1024

	config.OffsetAutoCommitInterval = 10 * time.Second
	config.OffsetAutoReset = sarama.OffsetNewest

	config.ClaimPartitionRetry = 5
	config.ClaimPartitionRetryInterval = 3 * time.Second

	return config, nil
}
