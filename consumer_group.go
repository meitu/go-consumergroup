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

	messageChans map[string]chan *sarama.ConsumerMessage
	errorChans   map[string]chan *sarama.ConsumerError

	logger *proxyLogger
	config *Config
}

// NewConsumerGroup create the ConsumerGroup instance with config
func NewConsumerGroup(config *Config) (*ConsumerGroup, error) {
	if config == nil {
		return nil, errors.New("config can't be empty")
	}
	err := config.validate()
	if err != nil {
		return nil, fmt.Errorf("vaildate config failed, as %s", err)
	}

	cg := new(ConsumerGroup)
	cg.state = cgInit
	cg.config = config
	cg.id = genConsumerID()
	cg.name = config.GroupID
	cg.topicList = config.TopicList
	cg.stopOnce = new(sync.Once)
	cg.stopper = make(chan struct{})
	cg.rebalanceOnce = new(sync.Once)
	cg.rebalanceTrigger = make(chan struct{})
	cg.storage = newZKGroupStorage(config.ZkList, config.ZkSessionTimeout)
	cg.messageChans = make(map[string]chan *sarama.ConsumerMessage)
	cg.errorChans = make(map[string]chan *sarama.ConsumerError)
	for _, topic := range cg.topicList {
		cg.messageChans[topic] = make(chan *sarama.ConsumerMessage)
		cg.errorChans[topic] = make(chan *sarama.ConsumerError, config.ErrorChannelBufferSize)
	}
	err = cg.initSaramaConsumer()
	if err != nil {
		return nil, fmt.Errorf("init sarama consumer, as %s", err)
	}
	prefix := fmt.Sprintf("[go-consumergroup %s]", cg.name)
	cg.logger = newProxyLogger(prefix, newDefaultLogger(infoLevel))
	return cg, nil
}

func (cg *ConsumerGroup) initSaramaConsumer() error {
	brokerList, err := cg.storage.getBrokerList()
	if err != nil {
		return err
	}
	if len(brokerList) == 0 {
		return errors.New("no broker alive")
	}
	cg.saramaConsumer, err = sarama.NewConsumer(brokerList, cg.config.SaramaConfig)
	return err
}

// SetLogger allow user to set user's logger, or defaultLogger would print to stdout.
func (cg *ConsumerGroup) SetLogger(logger Logger) {
	cg.logger.targetLogger = logger
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
		cg.logger.Errorf("[recover panic] %s %s", err, string(debug.Stack()))
		cg.ExitGroup()
	}
}

func (cg *ConsumerGroup) consumeTopicList() {
	var wg sync.WaitGroup

	defer func() {
		cg.state = cgStopped
		for _, topic := range cg.topicList {
			close(cg.messageChans[topic])
			close(cg.errorChans[topic])
		}
		err := cg.storage.deleteConsumer(cg.name, cg.id)
		if err != nil {
			cg.logger.Errorf("Failed to delete consumer from zookeeper, err %s", err)
		}
	}()

	defer cg.callRecover()

CONSUME_TOPIC_LOOP:
	for {
		cg.logger.Info("Consumer started")
		cg.rebalanceOnce = new(sync.Once)
		cg.stopOnce = new(sync.Once)

		err := cg.watchRebalance()
		if err != nil {
			cg.logger.Errorf("Failed to watch rebalance, err %s", err)
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
		case <-cg.rebalanceTrigger:
			cg.logger.Info("Trigger rebalance")
			cg.ExitGroup()
			wg.Wait()
			// The stopper channel was used to notify partition's consumer to stop consuming when rebalance is triggered.
			// So we should reinit when rebalace was triggered, as it would be closed.
			cg.stopper = make(chan struct{})
			cg.rebalanceTrigger = make(chan struct{})
			continue CONSUME_TOPIC_LOOP
		case <-cg.stopper: // triggered when ExitGroup() is called
			cg.logger.Info("ConsumeGroup is stopping")
			wg.Wait()
			cg.logger.Info("ConsumerGroup was stopped")
			return
		}
	}
}

func (cg *ConsumerGroup) consumeTopic(topic string) {
	var wg sync.WaitGroup
	cg.logger.Infof("Start to consume topic[%s]", topic)

	defer func() {
		cg.logger.Infof("Stop to consume topic[%s]", topic)
	}()

	partitions, err := cg.assignPartitions(topic)
	if err != nil {
		cg.logger.Errorf("Failed to assign partitions to topic[%s], err %s", topic, err)
		return
	}
	cg.logger.Infof("Topic[%s] Partitions %v are assigned to this consumer", topic, partitions)

	for _, partition := range partitions {
		wg.Add(1)
		cg.logger.Infof("Start to consume Topic[%s] partition[%d]", topic, partition)
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
	if cg.messageChans[topic] == nil {
		return nil, errors.New("topic was not found")
	}
	return cg.messageChans[topic], nil
}

// GetErrors was used to get a unbuffered error's channel from specified topic
func (cg *ConsumerGroup) GetErrors(topic string) (<-chan *sarama.ConsumerError, error) {
	if cg.errorChans[topic] == nil {
		return nil, errors.New("topic was not found")
	}
	return cg.errorChans[topic], nil
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
			cg.logger.Warnf("Failed to get topic[%s] partition[%d] owner, err %s", topic, partition, err)
		}
		if cg.id == owner {
			err := cg.storage.releasePartition(cg.name, topic, partition)
			if err != nil {
				cg.logger.Warnf("Failed to release topic[%s] partition[%d], err %s", topic, partition, err)
			}
		}
	}()

	for i := 0; i < cg.config.ClaimPartitionRetry; i++ {
		if err = cg.storage.claimPartition(cg.name, topic, partition, cg.id); err != nil {
			cg.logger.Warnf("Failed to claim topic[%s] partition[%d], err %s", topic, partition, err)
		}
		if err == nil {
			cg.logger.Infof("Claim topic[%s] partition[%d] success", topic, partition)
			break
		}
		time.Sleep(cg.config.ClaimPartitionRetryInterval)
	}
	if err != nil {
		cg.logger.Errorf("Failed to claim topic[%s] partition[%d] after %d retries", topic, partition, cg.config.ClaimPartitionRetry)
		cg.ExitGroup()
		return
	}

	nextOffset, err := cg.storage.getOffset(cg.name, topic, partition)
	if err != nil {
		cg.logger.Errorf("Failed to get topic[%s] partition[%d] offset, err %s", topic, partition, err)
		cg.ExitGroup()
		return
	}
	if nextOffset == -1 {
		nextOffset = cg.config.OffsetAutoReset
	}
	cg.logger.Debugf("Get topic[%s] partition[%d] offset[%d] from offset storage", topic, partition, nextOffset)

	consumer, err = cg.getPartitionConsumer(topic, partition, nextOffset)
	if err != nil {
		cg.logger.Errorf("Failed to get topic[%s] partition[%d] consumer, err %s", topic, partition, err)
		cg.ExitGroup()
		return
	}
	cg.logger.Infof("Topic[%s] partition[%d] Consumer has started", topic, partition)
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
						cg.logger.Warnf("Failed to commit topic[%s] partition[%d] offset, err %s", topic, partition, err)
					} else {
						cg.logger.Debugf("Commit topic[%s] partition[%d] offset[%d] to storage", topic, partition, offset)
						prevCommitOffset = offset
					}
				}
			}
		}()
	}

	messageChan := cg.messageChans[topic]
	errorChan := cg.errorChans[topic]

CONSUME_PARTITION_LOOP:
	for {
		select {
		case <-cg.stopper:
			break CONSUME_PARTITION_LOOP
		case error1 := <-consumer.Errors():
			errorChan <- error1
		case message := <-consumer.Messages():
			select {
			case messageChan <- message:
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
				cg.logger.Errorf("Failed to Commit topic[%s] partition[%d] offset[%d], err %s", topic, partition, nextOffset, err)
			} else {
				cg.logger.Debugf("Commit topic[%s] partition[%d] offset[%d] to storage", topic, partition, nextOffset)
			}
		}
	}

	cg.logger.Infof("Topic[%s] partition[%d] consumer was stopped", topic, partition)
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
	cg.logger.Info("The auto reconnect consumer goroutine was started")
	defer cg.logger.Info("The auto reconnect consumer goroutine was stopped")
	for {
		select {
		case <-cg.stopper:
			return
		case <-timer.C:
			timer.Reset(interval)
			exist, err := cg.storage.existsConsumer(cg.name, cg.id)
			if err != nil {
				cg.logger.Errorf("Failed to check consumer exist, err %s", err)
				break
			}
			if exist {
				break
			}
			err = cg.storage.registerConsumer(cg.name, cg.id, nil)
			if err != nil {
				cg.logger.Errorf("Faild to re-register consumer, err %s", err)
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
		cg.logger.Info("Rebalance checker was started")
		select {
		case <-consumerListChange:
			cg.logger.Info("Trigger rebalance while consumers was changed")
			cg.rebalanceOnce.Do(cg.triggerRebalance)
		case <-cg.stopper:
		}
		cg.logger.Info("Rebalance checker was exited")
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
		return nil, errors.New("no consumer was found")
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
