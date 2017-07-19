package consumergroup

import "github.com/samuel/go-zookeeper/zk"

type groupStorage interface {
	claimPartition(group, topic string, partition int32, consumerID string) error
	releasePartition(group, topic string, partition int32) error
	getPartitionOwner(group, topic string, partition int32) (string, error)
	registerConsumer(group, consumerID string, data []byte) error
	existsConsumer(group, consumerID string) (bool, error)
	deleteConsumer(group, consumerID string) error
	getBrokerList() ([]string, error)
	getConsumerList(group string) ([]string, error)
	watchConsumerList(group string) (<-chan zk.Event, error)
	commitOffset(group, topic string, partition int32, offset int64) error
	getOffset(group, topic string, partition int32) (int64, error)
}
