package consumergroup

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
)

// Config is used to pass configuration options to consumer groups. Items can
// be modified directly.
type Config struct {
	ZkList           []string
	ZkSessionTimeout time.Duration
	GroupID          string
	TopicList        []string

	SaramaConfig           *sarama.Config
	ErrorChannelBufferSize int

	OffsetAutoCommitEnable   bool
	OffsetAutoCommitInterval time.Duration
	OffsetAutoReset          int64

	ClaimPartitionRetry         int
	ClaimPartitionRetryInterval time.Duration
}

// NewConfig creates a new Config instance.
func NewConfig() *Config {
	config := new(Config)
	config.SaramaConfig = sarama.NewConfig()
	config.ErrorChannelBufferSize = 1024
	config.OffsetAutoCommitEnable = true
	config.OffsetAutoCommitInterval = 10 * time.Second
	config.OffsetAutoReset = sarama.OffsetNewest
	config.ClaimPartitionRetry = 5
	config.ClaimPartitionRetryInterval = 3 * time.Second
	return config
}

func (c *Config) validate() error {
	if c.ZkList == nil || len(c.ZkList) <= 0 {
		return errors.New("ZkList can't be empty")
	}
	if c.GroupID == "" {
		return errors.New("GroupID can't be empty")
	}
	if c.TopicList == nil || len(c.TopicList) <= 0 {
		return errors.New("GroupId can't be empty")
	}
	c.TopicList = sliceRemoveDuplicates(c.TopicList)
	c.ZkList = sliceRemoveDuplicates(c.ZkList)
	return nil
}
