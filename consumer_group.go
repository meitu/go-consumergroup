package consumergroup

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

// Constants defining consumer group's possible states.
const (
	CG_INIT = iota
	CG_START
	CG_STOPPED
)

type topicOffset map[int32]int64

// ConsumerGroup process Kafka messages from brokers. It supports group
// and rebalance.
type ConsumerGroup struct {
	name           string
	topicList      []string
	storage        GroupStorage
	saramaConsumer sarama.Consumer

	id               string
	state            int
	stopper          chan struct{}
	rebalanceTrigger chan struct{}
	stopOnce         *sync.Once
	rebalanceOnce    *sync.Once

	nextMessage map[string]chan *sarama.ConsumerMessage
	topicErrors map[string]chan *sarama.ConsumerError

	logger Logger
	config *Config
}

// NewConsumerGroup creates a new consumer group instance using the given
// group storage and config.
func NewConsumerGroup(storage GroupStorage, config *Config) (*ConsumerGroup, error) {
	var err error
	if storage == nil {
		return nil, errors.New("group storage can't be null")
	}

	if config == nil {
		return nil, errors.New("config can't be null")
	}

	cg := new(ConsumerGroup)

	cg.name = config.groupID
	cg.topicList = config.topicList
	cg.storage = storage
	brokerList, err := storage.GetBrokerList()
	if err != nil {
		return nil, fmt.Errorf("get brokerList failed: %s", err.Error())
	}
	if len(brokerList) == 0 {
		return nil, errors.New("no broker alive")
	}

	if cg.saramaConsumer, err = sarama.NewConsumer(brokerList, config.SaramaConfig); err != nil {
		return nil, fmt.Errorf("sarama consumer initialize failed, because %s", err.Error())
	}

	cg.state = CG_INIT
	cg.id = GenConsumerID()
	cg.stopper = make(chan struct{})
	cg.rebalanceTrigger = make(chan struct{})
	cg.rebalanceOnce = new(sync.Once)
	cg.stopOnce = new(sync.Once)

	cg.nextMessage = make(map[string]chan *sarama.ConsumerMessage)
	cg.topicErrors = make(map[string]chan *sarama.ConsumerError)
	for _, topic := range cg.topicList {
		cg.nextMessage[topic] = make(chan *sarama.ConsumerMessage)
		cg.topicErrors[topic] = make(chan *sarama.ConsumerError, config.ErrorChannelBufferSize)
	}

	cg.logger = NewInnerLog(INFO)
	cg.config = config
	return cg, nil
}

// SetLogger sets the logger and you need to implement the Logger interface first.
func (cg *ConsumerGroup) SetLogger(logger Logger) {
	cg.logger = logger
}

// JoinGroup registers a consumer to the consumer group and starts to
// process messages from the topic list.
func (cg *ConsumerGroup) JoinGroup() error {

	// the program exits if the consumer fails to register
	err := cg.storage.RegisterConsumer(cg.name, cg.id, nil)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	go cg.consumeTopicList()
	return nil
}

// ExitGroup close cg.stopper to notify consumer group stop consuming topic
// list.
func (cg *ConsumerGroup) ExitGroup() {
	cg.stopOnce.Do(func() { close(cg.stopper) })
}

// IsStopped returns true or false means if consumer group is stopped or not.
func (cg *ConsumerGroup) IsStopped() bool {
	return cg.state == CG_STOPPED
}

func (cg *ConsumerGroup) triggerRebalance() {
	close(cg.rebalanceTrigger)
}

func (cg *ConsumerGroup) callRecover() {
	if err := recover(); err != nil {
		cg.logger.Error(err, string(debug.Stack()))
		cg.ExitGroup()
	}
}

func (cg *ConsumerGroup) consumeTopicList() {
	var wg sync.WaitGroup

	defer func() {
		cg.state = CG_STOPPED
		for _, topic := range cg.topicList {
			close(cg.nextMessage[topic])
			close(cg.topicErrors[topic])
		}
		err := cg.storage.DeleteConsumer(cg.name, cg.id)
		if err != nil {
			cg.logger.Errorf("[go-consumergroup] delete consumer from zookeeper failed: %s\n", err.Error())
		}
	}()

	defer cg.callRecover()

CONSUME_TOPIC_LOOP:
	for {
		cg.logger.Infof("[go-consumergroup] consumer %s started\n", cg.id)
		cg.rebalanceOnce = new(sync.Once)
		cg.stopOnce = new(sync.Once)

		err := cg.checkRebalance()
		if err != nil {
			cg.logger.Errorf("[go-consumergroup] check rebalance failed: %s\n", err.Error())
			cg.ExitGroup()
			return
		}

		for _, topic := range cg.topicList {
			wg.Add(1)
			go func(topic string) {
				defer cg.callRecover()
				defer wg.Done()
				cg.consumeTopic(topic)
			}(topic)
		}

		cg.state = CG_START

		// waiting for restart or rebalance
		select {
		case <-cg.rebalanceTrigger:
			cg.logger.Info("[go-consumergroup] rebalance start")
			cg.ExitGroup()
			// stopper will be closed to notify partition consumers to
			// stop consuming when rebalance is triggered, and rebalanceTrigger
			// will also be closed to restart the consume topic loop in the meantime.
			wg.Wait()
			cg.stopper = make(chan struct{})
			cg.rebalanceTrigger = make(chan struct{})
			continue CONSUME_TOPIC_LOOP
		case <-cg.stopper: // triggered when ExitGroup() is called
			cg.logger.Info("[go-consumergroup] consumer shutting down")
			wg.Wait()
			cg.logger.Info("[go-consumergroup] consumer shut down")
			return
		}
	}
}

func (cg *ConsumerGroup) consumeTopic(topic string) {
	var wg sync.WaitGroup
	cg.logger.Infof("[go-consumergroup] start to consume topic [%s]\n", topic)

	defer func() {
		cg.logger.Infof("[go-consumergroup] stop to consume topic [%s]\n", topic)
	}()

	partitions, err := cg.assignPartitionToConsumer(topic)
	if err != nil {
		cg.logger.Errorf("[go-consumergroup] topic [%s] assign partition to consumer failed, %s", topic, err.Error())
		return
	}
	cg.logger.Infof("[go-consumergroup] [%s] partitions %v are assigned to this consumer\n", topic, partitions)

	for _, partition := range partitions {
		wg.Add(1)
		cg.logger.Infof("[go-consumergroup] [%s, %d] consumer start\n", topic, partition)
		go func(topic string, partition int32) {
			defer cg.callRecover()
			defer wg.Done()
			cg.consumePartition(topic, partition)
		}(topic, partition)
	}

	wg.Wait()
}

func (cg *ConsumerGroup) getPartitionConsumer(topic string, partition int32, nextOffset int64) (sarama.PartitionConsumer, error) {
	consumer, err := cg.saramaConsumer.ConsumePartition(topic, partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
		nextOffset = cg.config.OffsetAutoReset
		consumer, err = cg.saramaConsumer.ConsumePartition(topic, partition, nextOffset)
	}
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

// GetTopicNextMessageChannel returns a unbuffered channel from which to get
// messages of a specified topic.
func (cg *ConsumerGroup) GetTopicNextMessageChannel(topic string) (<-chan *sarama.ConsumerMessage, error) {
	if cg.nextMessage[topic] == nil {
		return nil, errors.New("have not found this topic in this cluster")
	}
	return cg.nextMessage[topic], nil
}

// GetTopicErrorsChannel returns a buffered channel from which to get error
// messages of a specified topic.
func (cg *ConsumerGroup) GetTopicErrorsChannel(topic string) (<-chan *sarama.ConsumerError, error) {
	if cg.topicErrors[topic] == nil {
		return nil, errors.New("have not found this topis in this cluster")
	}
	return cg.topicErrors[topic], nil
}

func (cg *ConsumerGroup) consumePartition(topic string, partition int32) {
	var err error
	var consumer sarama.PartitionConsumer
	var mutex sync.Mutex
	var wg sync.WaitGroup

	select {
	case <-cg.stopper:
		return
	default:
	}

	defer func() {
		owner, err := cg.storage.GetPartitionOwner(cg.name, topic, partition)
		if err != nil {
			cg.logger.Warnf("[go-consumergroup] [%s, %d] Get partition owner failed: %s\n", topic, partition, err.Error())
		}
		if cg.id == owner {
			err := cg.storage.ReleasePartition(cg.name, topic, partition)
			if err != nil {
				cg.logger.Warnf("[go-consumergroup] [%s, %d] release partition failed: %s\n", topic, partition, err.Error())
			}
		}
	}()

	for i := 0; i < cg.config.ClaimPartitionRetry; i++ {
		if err = cg.storage.ClaimPartition(cg.name, topic, partition, cg.id); err != nil {
			cg.logger.Warnf("[go-consumergroup] [%s, %d] Claim partition failed: %s\n", topic, partition, err.Error())
		}
		if err == nil {
			cg.logger.Infof("[go-consumergroup] [%s, %d] Claim partition succeeded!\n", topic, partition)
			break
		}
		time.Sleep(cg.config.ClaimPartitionRetryInterval)
	}
	if err != nil {
		cg.logger.Errorf("[go-consumergroup] [%s, %d] Claim partition failed after %d retries\n", topic, partition, cg.config.ClaimPartitionRetry)
		cg.ExitGroup()
		return
	}

	nextOffset, err := cg.storage.GetOffset(cg.name, topic, partition)
	if err != nil {
		cg.logger.Errorf("[go-consumergroup] [%s, %d] get offset failed: %s\n", topic, partition, err.Error())
		cg.ExitGroup()
		return
	}
	if nextOffset == -1 {
		nextOffset = cg.config.OffsetAutoReset
	}
	cg.logger.Debugf("[go-consumergroup] [%s, %d] get offset %d from offset storage\n", topic, partition, nextOffset)

	consumer, err = cg.getPartitionConsumer(topic, partition, nextOffset)
	if err != nil {
		cg.logger.Errorf("[go-consumergroup] [%s, %d] get partition consumer failed: %s\n", topic, partition, err.Error())
		cg.ExitGroup()
		return
	}
	cg.logger.Infof("[go-consumergroup] [%s, %d] consumer has started\n", topic, partition)
	defer consumer.Close()

	prevCommitOffset := nextOffset

	wg.Add(1)
	go func() {
		defer cg.callRecover()
		defer wg.Done()
		timer := time.NewTimer(cg.config.OffsetAutoCommitInterval)
		for {
			select {
			case <-cg.stopper:
				return
			case <-timer.C:
				timer.Reset(cg.config.OffsetAutoCommitInterval)
				mutex.Lock()
				offset := nextOffset
				mutex.Unlock()

				if offset == prevCommitOffset {
					break
				}
				err := cg.storage.CommitOffset(cg.name, topic, partition, offset)
				if err != nil {
					cg.logger.Warnf("[go-consumergroup] [%s, %d] commit offset failed: %s\n", topic, partition, err.Error())
				} else {
					cg.logger.Debugf("[go-consumergroup] [%s, %d] commit offset %d to offset storage\n", topic, partition, offset)
					prevCommitOffset = offset
				}
			}
		}
	}()

	nextMessage := cg.nextMessage[topic]
	errors := cg.topicErrors[topic]

CONSUME_PARTITION_LOOP:
	for {
		select {
		case <-cg.stopper:
			break CONSUME_PARTITION_LOOP

		case error1 := <-consumer.Errors():
			errors <- error1
			//sends error messages to the error channel

		case message := <-consumer.Messages():
			select {
			case nextMessage <- message:
				mutex.Lock()
				nextOffset = message.Offset + 1
				mutex.Unlock()
			case <-cg.stopper:
				break CONSUME_PARTITION_LOOP
			}
		}
	}

	wg.Wait()

	if nextOffset != prevCommitOffset {
		err = cg.storage.CommitOffset(cg.name, topic, partition, nextOffset)
		if err != nil {
			cg.logger.Errorf("[go-consumergroup] [%s, %d] current offset %d commit offset failed: %s\n", topic, partition, nextOffset, err.Error())
		} else {
			cg.logger.Debugf("[go-consumergroup] [%s, %d] commit offset %d to offset storage\n", topic, partition, nextOffset)
		}
	}

	cg.logger.Infof("[go-consumergroup] [%s, %d] consumer has stopped\n", topic, partition)
}

func (cg *ConsumerGroup) getPartitionNum(topic string) (int, error) {
	partitions, err := cg.saramaConsumer.Partitions(topic)
	if err != nil {
		return 0, err
	}
	return len(partitions), nil
}

func (cg *ConsumerGroup) checkRebalance() error {
	consumerListChange, err := cg.storage.WatchConsumerList(cg.name)
	if err != nil {
		return err
	}

	go func() {
		defer cg.callRecover()
		defer cg.logger.Info("[go-consumergroup] rebalance checker exited")

		cg.logger.Info("[go-consumergroup] rebalance checker started")
		select {
		case <-consumerListChange:
			cg.logger.Info("[go-consumergroup] trigger rebalance because consumer list change")
			cg.rebalanceOnce.Do(cg.triggerRebalance)
		case <-cg.stopper:
		}
	}()

	return nil
}

func (cg *ConsumerGroup) assignPartitionToConsumer(topic string) ([]int32, error) {
	j := 0
	partNum, err := cg.getPartitionNum(topic)
	if err != nil || partNum == 0 {
		return nil, err
	}
	consumerList, err := cg.storage.GetConsumerList(cg.name)
	if err != nil {
		return nil, err
	}
	consumerNum := len(consumerList)
	if consumerNum == 0 {
		return nil, errors.New("no consumer is found")
	}
	partitions := make([]int32, 0, partNum/consumerNum+1)
	for i := 0; i < partNum; i++ {
		id := consumerList[j%consumerNum]
		if id == cg.id {
			partitions = append(partitions, int32(i))
		}
		j++
	}
	return partitions, nil
}
