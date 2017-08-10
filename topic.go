package consumergroup

import (
	"errors"
	"sync"

	"github.com/Shopify/sarama"
)

type topicConsumer struct {
	group              string
	name               string
	owner              *ConsumerGroup
	errors             chan *sarama.ConsumerError
	messages           chan *sarama.ConsumerMessage
	partitionConsumers map[int32]*partitionConsumer
}

func newTopicConsumer(owner *ConsumerGroup, topic string) *topicConsumer {
	tc := new(topicConsumer)
	tc.owner = owner
	tc.name = topic
	tc.errors = make(chan *sarama.ConsumerError)
	tc.messages = make(chan *sarama.ConsumerMessage)
	return tc
}

func (tc *topicConsumer) start() {
	var wg sync.WaitGroup

	cg := tc.owner
	topic := tc.name
	cg.logger.Infof("Start to consume topic[%s]", topic)
	defer cg.logger.Infof("Stop to consume topic[%s]", topic)

	partitions, err := tc.assignPartitions()
	if err != nil {
		cg.logger.Errorf("Failed to assign partitions to topic[%s], err %s", topic, err)
		return
	}
	cg.logger.Infof("Topic[%s], partitions %v are assigned to this consumer", topic, partitions)
	tc.partitionConsumers = make(map[int32]*partitionConsumer)
	for _, partition := range partitions {
		tc.partitionConsumers[partition] = newPartitionConsumer(tc, partition)
	}
	for partition, consumer := range tc.partitionConsumers {
		wg.Add(1)
		go func(pc *partitionConsumer) {
			defer cg.callRecover()
			defer wg.Done()
			pc.start()
		}(consumer)
		cg.logger.Infof("Start to consume Topic[%s] partition[%d]", topic, partition)
	}
	wg.Wait()
}

func (tc *topicConsumer) assignPartitions() ([]int32, error) {
	var partitions []int32

	cg := tc.owner
	partNum, err := tc.getPartitionNum()
	if err != nil || partNum == 0 {
		return nil, err
	}
	consumerList, err := cg.storage.getConsumerList(cg.name)
	if err != nil {
		return nil, err
	}
	consumerNum := len(consumerList)
	if consumerNum == 0 {
		return nil, errors.New("no consumer was found")
	}
	for i := int32(0); i < partNum; i++ {
		id := consumerList[i%int32(consumerNum)]
		if id == cg.id {
			partitions = append(partitions, i)
		}
	}
	return partitions, nil
}

func (tc *topicConsumer) getPartitionNum() (int32, error) {
	partitions, err := tc.owner.saramaConsumer.Partitions(tc.name)
	if err != nil {
		return 0, err
	}
	return int32(len(partitions)), nil
}
