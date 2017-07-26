package consumergroup

import (
	"errors"
	"time"

	"github.com/Shopify/sarama"
)

type Config struct {
	// ZkList is required, zookeeper address's list
	ZkList []string
	// Zookeeper session timeout, default is 6s
	ZkSessionTimeout time.Duration
	// GroupID is required, identifer to determin which ConsumerGroup would be joined
	GroupID string
	// TopicList is required, topics that ConsumerGroup would be consumed
	TopicList []string
	// Just export Sarama Config
	SaramaConfig *sarama.Config
	// Size of error channel, default is 1024
	ErrorChannelBufferSize int
	// Whether auto commit the offset or not, default is true
	OffsetAutoCommitEnable bool
	// Offset auto commit interval, default is 10s
	OffsetAutoCommitInterval time.Duration
	// Where to fetch messages when offset was not found, default is newest
	OffsetAutoReset int64
	// Claim the partition would give up after ClaimPartitionRetryTimes(>0) retires,
	// ClaimPartitionRetryTimes <= 0 would retry until success or receive stop signal
	ClaimPartitionRetryTimes int
	// Retry interval when fail to clain the partition
	ClaimPartitionRetryInterval time.Duration
}

// NewConfig return the new config with default value.
func NewConfig() *Config {
	config := new(Config)
	config.SaramaConfig = sarama.NewConfig()
	config.ErrorChannelBufferSize = 1024
	config.OffsetAutoCommitEnable = true
	config.OffsetAutoCommitInterval = 10 * time.Second
	config.OffsetAutoReset = sarama.OffsetNewest
	config.ClaimPartitionRetryTimes = 10
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
