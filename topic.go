package consumergroup

import (
	"errors"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

type topicConsumer struct {
	group              string
	name               string
	owner              *ConsumerGroup
	errors             chan *sarama.ConsumerError
	messages           chan *sarama.ConsumerMessage
	partitionConsumers map[int32]*partitionConsumer
	wg                 sync.WaitGroup
}

func newTopicConsumer(owner *ConsumerGroup, topic string) *topicConsumer {
	tc := new(topicConsumer)
	tc.owner = owner
	tc.group = owner.name
	tc.name = topic
	tc.errors = make(chan *sarama.ConsumerError)
	tc.messages = make(chan *sarama.ConsumerMessage)
	return tc
}

func (tc *topicConsumer) start() {

	cg := tc.owner
	topic := tc.name

	cg.logger.WithFields(logrus.Fields{
		"group": tc.group,
		"topic": topic,
	}).Info("Start the topic consumer")

	partitions, err := tc.assignPartitions()
	if err != nil {
		cg.logger.WithFields(logrus.Fields{
			"group": tc.group,
			"topic": topic,
			"err":   err,
		}).Error("Failed to assign partitions to topic consumer")
		return
	}

	cg.logger.WithFields(logrus.Fields{
		"group":      tc.group,
		"topic":      topic,
		"partitions": partitions,
	}).Info("The partitions was assigned to current topic consumer")
	tc.partitionConsumers = make(map[int32]*partitionConsumer)
	for _, partition := range partitions {
		tc.partitionConsumers[partition] = newPartitionConsumer(tc, partition)
	}
	for partition, consumer := range tc.partitionConsumers {
		tc.wg.Add(1)
		go func(pc *partitionConsumer) {
			defer cg.callRecover()
			defer tc.wg.Done()
			pc.start()
		}(consumer)
		cg.logger.WithFields(logrus.Fields{
			"group":     tc.group,
			"topic":     topic,
			"partition": partition,
		}).Info("Topic consumer start to consume the partition")
	}
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
		cg.owners[tc.name][i] = id
		if id == cg.id {
			partitions = append(partitions, i)
		}
	}
	return partitions, nil
}

func (tc *topicConsumer) getPartitionNum() (int32, error) {
	if saramaConsumer, ok := tc.owner.saramaConsumers[tc.name]; !ok {
		return 0, errors.New("sarama conumser was not found")
	} else {
		partitions, err := saramaConsumer.Partitions(tc.name)
		if err != nil {
			return 0, err
		}
		return int32(len(partitions)), nil
	}
}

func (tc *topicConsumer) getOffsets() map[int32]interface{} {
	partitions := make(map[int32]interface{})
	for partition, pc := range tc.partitionConsumers {
		partitions[partition] = pc.getOffset()
	}
	return partitions
}
