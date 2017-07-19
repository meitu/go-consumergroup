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

const (
	cgInit = iota
	cgStart
	cgStopped
)

// ConsumerGroup consume message from Kafka with rebalancing supports
type ConsumerGroup struct {
	name           string
	topicList      []string
	storage        groupStorage
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

// NewConsumerGroup create the ConsumerGroup instance with config
func NewConsumerGroup(config *Config) (*ConsumerGroup, error) {
	if config == nil {
		return nil, errors.New("config can't be null")
	}
	err := config.validate()
	if err != nil {
		return nil, fmt.Errorf("vaildate config failed, as %s", err)
	}

	cg := new(ConsumerGroup)
	cg.name = config.GroupID
	cg.topicList = config.TopicList
	cg.storage = newZKGroupStorage(config.ZkList, config.ZkSessionTimeout)
	brokerList, err := cg.storage.getBrokerList()
	if err != nil {
		return nil, fmt.Errorf("get brokerList failed: %s", err.Error())
	}
	if len(brokerList) == 0 {
		return nil, errors.New("no broker alive")
	}

	if cg.saramaConsumer, err = sarama.NewConsumer(brokerList, config.SaramaConfig); err != nil {
		return nil, fmt.Errorf("sarama consumer initialize failed, because %s", err.Error())
	}

	cg.state = cgInit
	cg.id = genConsumerID()
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
	cg.logger = newDefaultLogger(infoLevel)
	cg.config = config
	return cg, nil
}

// SetLogger allow user to set user's logger, or defaultLogger would print to stdout.
func (cg *ConsumerGroup) SetLogger(logger Logger) {
	cg.logger = logger
}

// JoinGroup would register ConsumerGroup, and rebalance would be triggered.
// ConsumerGroup computes the partitions which should be consumed by consumer's num, and start fetching message.
func (cg *ConsumerGroup) JoinGroup() error {

	// exit when failed to register the consumer
	err := cg.storage.registerConsumer(cg.name, cg.id, nil)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	go cg.consumeTopicList()
	return nil
}

// ExitGroup would unregister ConsumerGroup, and rebalance would be triggered.
// The partitions which consumed by this ConsumerGroup would be assigned to others.
func (cg *ConsumerGroup) ExitGroup() {
	cg.stopOnce.Do(func() { close(cg.stopper) })
}

// IsStopped return whether the ConsumerGroup was stopped or not.
func (cg *ConsumerGroup) IsStopped() bool {
	return cg.state == cgStopped
}

func (cg *ConsumerGroup) triggerRebalance() {
	close(cg.rebalanceTrigger)
}

func (cg *ConsumerGroup) callRecover() {
	if err := recover(); err != nil {
		cg.logger.Errorf("[go-consumergroup] [%s] %s %s", cg.name, err, string(debug.Stack()))
		cg.ExitGroup()
	}
}

func (cg *ConsumerGroup) consumeTopicList() {
	var wg sync.WaitGroup

	defer func() {
		cg.state = cgStopped
		for _, topic := range cg.topicList {
			close(cg.nextMessage[topic])
			close(cg.topicErrors[topic])
		}
		err := cg.storage.deleteConsumer(cg.name, cg.id)
		if err != nil {
			cg.logger.Errorf("[go-consumergroup] [%s] delete consumer from zookeeper failed: %s", cg.name, err.Error())
		}
	}()

	defer cg.callRecover()

CONSUME_TOPIC_LOOP:
	for {
		cg.logger.Infof("[go-consumergroup] [%s] consumer started", cg.name)
		cg.rebalanceOnce = new(sync.Once)
		cg.stopOnce = new(sync.Once)

		err := cg.watchRebalance()
		if err != nil {
			cg.logger.Errorf("[go-consumergroup] [%s] check rebalance failed: %s", cg.name, err.Error())
			cg.ExitGroup()
			return
		}

		wg.Add(1)
		go func() {
			defer cg.callRecover()
			defer wg.Done()
			cg.autoReconnect(cg.storage.(*zkGroupStorage).sessionTimeout / 3)
		}()

		for _, topic := range cg.topicList {
			wg.Add(1)
			go func(topic string) {
				defer cg.callRecover()
				defer wg.Done()
				cg.consumeTopic(topic)
			}(topic)
		}

		cg.state = cgStart

		select {
		case <-cg.rebalanceTrigger: // Waiting for restart/rebalance
			cg.logger.Infof("[go-consumergroup] [%s] rebalance start", cg.name)
			cg.ExitGroup()
			wg.Wait()
			// The stopper channel was used to notify partition's consumer to stop consuming when rebalance is triggered.
			// So we should reinit when rebalace was triggered, as it would be closed.
			cg.stopper = make(chan struct{})
			cg.rebalanceTrigger = make(chan struct{})
			continue CONSUME_TOPIC_LOOP
		case <-cg.stopper: // Triggered when ExitGroup() was called
			cg.logger.Infof("[go-consumergroup] [%s] consumer shutting down", cg.name)
			wg.Wait()
			cg.logger.Infof("[go-consumergroup] [%s] consumer shut down", cg.name)
			return
		}
	}
}

func (cg *ConsumerGroup) consumeTopic(topic string) {
	var wg sync.WaitGroup
	cg.logger.Infof("[go-consumergroup] [%s, %s] start to consume", cg.name, topic)

	defer func() {
		cg.logger.Infof("[go-consumergroup] [%s, %s] stop to consume", cg.name, topic)
	}()

	partitions, err := cg.assignPartitions(topic)
	if err != nil {
		cg.logger.Errorf("[go-consumergroup] [%s, %s] assign partition to consumer failed: %s", cg.name, topic, err.Error())
		return
	}
	cg.logger.Infof("[go-consumergroup] [%s, %s] partitions %v are assigned to this consumer", cg.name, topic, partitions)

	for _, partition := range partitions {
		wg.Add(1)
		cg.logger.Infof("[go-consumergroup] [%s, %s, %d] consumer start", cg.name, topic, partition)
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

// GetMessages was used to get a unbuffered message's channel from specified topic
func (cg *ConsumerGroup) GetMessages(topic string) (<-chan *sarama.ConsumerMessage, error) {
	if cg.nextMessage[topic] == nil {
		return nil, errors.New("have not found this topic in this cluster")
	}
	return cg.nextMessage[topic], nil
}

// GetErrors was used to get a unbuffered error's channel from specified topic
func (cg *ConsumerGroup) GetErrors(topic string) (<-chan *sarama.ConsumerError, error) {
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
		owner, err := cg.storage.getPartitionOwner(cg.name, topic, partition)
		if err != nil {
			cg.logger.Warnf("[go-consumergroup] [%s, %s, %d] Get partition owner failed: %s", cg.name, topic, partition, err.Error())
		}
		if cg.id == owner {
			err := cg.storage.releasePartition(cg.name, topic, partition)
			if err != nil {
				cg.logger.Warnf("[go-consumergroup] [%s, %s, %d] release partition failed: %s", cg.name, topic, partition, err.Error())
			}
		}
	}()

	for i := 0; i < cg.config.ClaimPartitionRetry; i++ {
		if err = cg.storage.claimPartition(cg.name, topic, partition, cg.id); err != nil {
			cg.logger.Warnf("[go-consumergroup] [%s, %s, %d] Claim partition failed: %s", cg.name, topic, partition, err.Error())
		}
		if err == nil {
			cg.logger.Infof("[go-consumergroup] [%s, %s, %d] Claim partition succeeded!", cg.name, topic, partition)
			break
		}
		time.Sleep(cg.config.ClaimPartitionRetryInterval)
	}
	if err != nil {
		cg.logger.Errorf("[go-consumergroup] [%s, %s, %d] Claim partition failed after %d retries", cg.name, topic, partition, cg.config.ClaimPartitionRetry)
		cg.ExitGroup()
		return
	}

	nextOffset, err := cg.storage.getOffset(cg.name, topic, partition)
	if err != nil {
		cg.logger.Errorf("[go-consumergroup] [%s, %s, %d] get offset failed: %s", cg.name, topic, partition, err.Error())
		cg.ExitGroup()
		return
	}
	if nextOffset == -1 {
		nextOffset = cg.config.OffsetAutoReset
	}
	cg.logger.Debugf("[go-consumergroup] [%s, %s, %d] get offset %d from offset storage", cg.name, topic, partition, nextOffset)

	consumer, err = cg.getPartitionConsumer(topic, partition, nextOffset)
	if err != nil {
		cg.logger.Errorf("[go-consumergroup] [%s, %s, %d] get partition consumer failed: %s", cg.name, topic, partition, err.Error())
		cg.ExitGroup()
		return
	}
	cg.logger.Infof("[go-consumergroup] [%s, %s, %d] consumer has started", cg.name, topic, partition)
	defer consumer.Close()

	prevCommitOffset := nextOffset

	if cg.config.OffsetAutoCommitEnable {
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
					err := cg.storage.commitOffset(cg.name, topic, partition, offset)
					if err != nil {
						cg.logger.Warnf("[go-consumergroup] [%s, %s, %d] commit offset failed: %s", cg.name, topic, partition, err.Error())
					} else {
						cg.logger.Debugf("[go-consumergroup] [%s, %s, %d] commit offset %d to offset storage", cg.name, topic, partition, offset)
						prevCommitOffset = offset
					}
				}
			}
		}()
	}

	nextMessage := cg.nextMessage[topic]
	errors := cg.topicErrors[topic]

CONSUME_PARTITION_LOOP:
	for {
		select {
		case <-cg.stopper:
			break CONSUME_PARTITION_LOOP

		case error1 := <-consumer.Errors():
			errors <- error1

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

	if cg.config.OffsetAutoCommitEnable {
		if nextOffset != prevCommitOffset {
			err = cg.storage.commitOffset(cg.name, topic, partition, nextOffset)
			if err != nil {
				cg.logger.Errorf("[go-consumergroup] [%s, %s, %d] current offset %d commit offset failed: %s", cg.name, topic, partition, nextOffset, err.Error())
			} else {
				cg.logger.Debugf("[go-consumergroup] [%s, %s, %d] commit offset %d to offset storage", cg.name, topic, partition, nextOffset)
			}
		}
	}

	cg.logger.Infof("[go-consumergroup] [%s, %s, %d] consumer has stopped", cg.name, topic, partition)
}

func (cg *ConsumerGroup) getPartitionNum(topic string) (int, error) {
	partitions, err := cg.saramaConsumer.Partitions(topic)
	if err != nil {
		return 0, err
	}
	return len(partitions), nil
}

func (cg *ConsumerGroup) autoReconnect(interval time.Duration) {
	timer := time.NewTimer(interval)
	cg.logger.Infof("[go-consumergroup] [%s] auto reconnect consumer goroutine start", cg.name)
	defer cg.logger.Infof("[go-consumergroup] [%s] auto reconnect consumer goroutine stop", cg.name)
	for {
		select {
		case <-cg.stopper:
			return
		case <-timer.C:
			timer.Reset(interval)
			exist, err := cg.storage.existsConsumer(cg.name, cg.id)
			if err != nil {
				cg.logger.Errorf("[go-consumergroup] [%s] check consumer exist failed: %s", cg.name, err.Error())
				break
			}
			if exist {
				break
			}
			err = cg.storage.registerConsumer(cg.name, cg.id, nil)
			if err != nil {
				cg.logger.Errorf("[go-consumergroup] [%s] re-register consumer failed: %s", cg.name, err.Error())
			}
		}
	}
}

func (cg *ConsumerGroup) watchRebalance() error {
	consumerListChange, err := cg.storage.watchConsumerList(cg.name)
	if err != nil {
		return err
	}

	go func() {
		defer cg.callRecover()
		defer cg.logger.Infof("[go-consumergroup] [%s] rebalance checker exited", cg.name)

		cg.logger.Infof("[go-consumergroup] [%s] rebalance checker started", cg.name)
		select {
		case <-consumerListChange:
			cg.logger.Infof("[go-consumergroup] [%s] trigger rebalance because consumer list change", cg.name)
			cg.rebalanceOnce.Do(cg.triggerRebalance)
		case <-cg.stopper:
		}
	}()

	return nil
}

func (cg *ConsumerGroup) assignPartitions(topic string) ([]int32, error) {
	j := 0
	partNum, err := cg.getPartitionNum(topic)
	if err != nil || partNum == 0 {
		return nil, err
	}
	consumerList, err := cg.storage.getConsumerList(cg.name)
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

// CommitOffset is used to commit offset when auto commit was disabled.
func (cg *ConsumerGroup) CommitOffset(topic string, partition int32, offset int64) error {
	if cg.config.OffsetAutoCommitEnable {
		return errors.New("commit offset take effect when offset auto commit was disabled")
	}
	return cg.storage.commitOffset(cg.name, topic, partition, offset)
}
