package consumergroup

import "github.com/samuel/go-zookeeper/zk"

type GroupStorage interface {
	ClaimPartition(group, topic string, partition int32, consumerId string) error
	ReleasePartition(group, topic string, partition int32) error
	GetPartitionOwner(group, topic string, partition int32) (string, error)
	RegisterConsumer(group, consumerId string, data []byte) error
	DeleteConsumer(group, consumerId string) error
	GetBrokerList() ([]string, error)
	GetConsumerList(group string) ([]string, error)
	WatchConsumerList(group string) (<-chan zk.Event, error)
	CommitOffset(group, topic string, partition int32, offset int64) error
	GetOffset(group, topic string, partition int32) (int64, error)
}
