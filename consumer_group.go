package consumergroup

import (
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/meitu/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

const (
	cgInit = iota
	cgStart
	cgStopped
)

const (
	restartEvent = 1
)

// ConsumerGroup consume message from Kafka with rebalancing supports
type ConsumerGroup struct {
	name            string
	storage         groupStorage
	topicConsumers  map[string]*topicConsumer
	saramaClients   map[string]sarama.Client
	saramaConsumers map[string]sarama.Consumer

	id          string
	state       int
	wg          sync.WaitGroup
	stopCh      chan struct{}
	triggerCh   chan int
	triggerOnce *sync.Once
	owners      map[string]map[int32]string

	config *Config
	logger *logrus.Logger

	onLoad, onClose []func()
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
	cg.id = config.ConsumerID
	if cg.id == "" {
		cg.id = genConsumerID()
	}
	cg.name = config.GroupID
	cg.triggerCh = make(chan int)
	cg.topicConsumers = make(map[string]*topicConsumer)
	cg.saramaClients = make(map[string]sarama.Client)
	cg.saramaConsumers = make(map[string]sarama.Consumer)
	cg.onLoad = make([]func(), 0)
	cg.onClose = make([]func(), 0)
	cg.storage = newZKGroupStorage(config.ZkList, config.ZkSessionTimeout)
	cg.logger = logrus.New()
	if _, ok := cg.storage.(*zkGroupStorage); ok {
		cg.storage.(*zkGroupStorage).Chroot(config.Chroot)
	}

	err = cg.initSaramaConsumer()
	if err != nil {
		return nil, fmt.Errorf("init sarama consumer, as %s", err)
	}
	cg.owners = make(map[string]map[int32]string)
	for _, topic := range config.TopicList {
		cg.topicConsumers[topic] = newTopicConsumer(cg, topic)
		cg.owners[topic] = make(map[int32]string)
	}
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
	for _, topic := range cg.config.TopicList {
		saramaClient, err := sarama.NewClient(brokerList, cg.config.SaramaConfig)
		if err != nil {
			return err
		}
		saramaConsumer, err := sarama.NewConsumerFromClient(saramaClient)
		if err != nil {
			return err
		}
		cg.saramaClients[topic] = saramaClient
		cg.saramaConsumers[topic] = saramaConsumer
	}
	return nil
}

// Start would register ConsumerGroup, and rebalance would be triggered.
// ConsumerGroup computes the partitions which should be consumed by consumer's num, and start fetching message.
func (cg *ConsumerGroup) Start() error {
	// exit when failed to register the consumer
	err := cg.storage.registerConsumer(cg.name, cg.id, nil)
	if err != nil && err != zk.ErrNodeExists {
		return err
	}
	cg.wg.Add(1)
	go cg.start()
	return nil
}

// Stop would unregister ConsumerGroup, and rebalance would be triggered.
// The partitions which consumed by this ConsumerGroup would be assigned to others.
func (cg *ConsumerGroup) Stop() {
	cg.stop()
	cg.wg.Wait()
}

// SetLogger use to set the user's logger the consumer group
func (cg *ConsumerGroup) SetLogger(l *logrus.Logger) {
	if l != nil {
		cg.logger = l
	}
}

// IsStopped return whether the ConsumerGroup was stopped or not.
func (cg *ConsumerGroup) IsStopped() bool {
	return cg.state == cgStopped
}

func (cg *ConsumerGroup) callRecover() {
	if err := recover(); err != nil {
		cg.logger.WithFields(logrus.Fields{
			"group": cg.name,
			"err":   err,
			"stack": string(debug.Stack()),
		}).Error("Recover panic")
		cg.stop()
	}
}

func (cg *ConsumerGroup) start() {
	var wg sync.WaitGroup

	defer cg.callRecover()
	defer func() {
		cg.state = cgStopped
		err := cg.storage.deleteConsumer(cg.name, cg.id)
		if err != nil {
			cg.logger.WithFields(logrus.Fields{
				"group": cg.name,
				"err":   err,
			}).Error("Failed to delete consumer from zk")
		}
		for _, tc := range cg.topicConsumers {
			close(tc.messages)
			close(tc.errors)
		}
		cg.wg.Done()
	}()

CONSUME_TOPIC_LOOP:
	for {
		cg.logger.WithField("group", cg.name).Info("Consumer group started")
		cg.triggerOnce = new(sync.Once)
		cg.stopCh = make(chan struct{})

		err := cg.watchRebalance()
		if err != nil {
			cg.logger.WithFields(logrus.Fields{
				"group": cg.name,
				"err":   err,
			}).Error("Failed to watch rebalance")
			cg.stop()
			return
		}
		wg.Add(1)
		go func() {
			defer cg.callRecover()
			defer wg.Done()
			cg.autoReconnect(cg.storage.(*zkGroupStorage).sessionTimeout / 3)
		}()
		for _, consumer := range cg.topicConsumers {
			wg.Add(1)
			consumer.start()
			go func(tc *topicConsumer) {
				defer cg.callRecover()
				defer wg.Done()
				tc.wg.Wait()
				cg.logger.WithFields(logrus.Fields{
					"group": tc.group,
					"topic": tc.name,
				}).Info("Stop the topic consumer")
			}(consumer)
		}
		cg.state = cgStart
		for _, onLoadFunc := range cg.onLoad {
			onLoadFunc()
		}
		msg := <-cg.triggerCh
		for _, onCloseFunc := range cg.onClose {
			onCloseFunc()
		}
		switch msg {
		case restartEvent:
			close(cg.stopCh)
			// The stop channel was used to notify partition's consumer to stop consuming when rebalance is triggered.
			// So we should reinit when rebalance was triggered, as it would be closed.
			wg.Wait()
			continue CONSUME_TOPIC_LOOP
		default:
			close(cg.stopCh)
			cg.logger.WithField("group", cg.name).Info("ConsumerGroup is stopping")
			wg.Wait()
			cg.logger.WithField("group", cg.name).Info("ConsumerGroup was stopped")
			return
		}
	}
}

func (cg *ConsumerGroup) stop() {
	cg.triggerOnce.Do(func() { close(cg.triggerCh) })
}

func (cg *ConsumerGroup) triggerRebalance() {
	cg.triggerOnce.Do(func() { cg.triggerCh <- restartEvent })
}

func (cg *ConsumerGroup) getPartitionConsumer(topic string, partition int32, nextOffset int64) (sarama.PartitionConsumer, error) {
	saramaConsumer := cg.saramaConsumers[topic]
	consumer, err := saramaConsumer.ConsumePartition(topic, partition, nextOffset)
	if err == sarama.ErrOffsetOutOfRange {
		cg.logger.WithFields(logrus.Fields{
			"group":     cg.name,
			"topic":     topic,
			"partition": partition,
			"offset":    nextOffset,
		}).Error("Partition's offset was out of range, use auto-reset")
		nextOffset = cg.config.OffsetAutoReset
		consumer, err = saramaConsumer.ConsumePartition(topic, partition, nextOffset)
	}
	return consumer, err
}

// GetMessages was used to get a unbuffered message's channel from specified topic
func (cg *ConsumerGroup) GetMessages(topic string) (<-chan *sarama.ConsumerMessage, bool) {
	if topicConsumer, ok := cg.topicConsumers[topic]; ok {
		return topicConsumer.messages, true
	}
	return nil, false
}

// GetErrors was used to get a unbuffered error's channel from specified topic
func (cg *ConsumerGroup) GetErrors(topic string) (<-chan *sarama.ConsumerError, bool) {
	if topicConsumer, ok := cg.topicConsumers[topic]; ok {
		return topicConsumer.errors, true
	}
	return nil, false
}

// OnLoad load callback function that runs after startup
func (cg *ConsumerGroup) OnLoad(cb func()) {
	cg.onLoad = append(cg.onLoad, cb)
}

// OnClose load callback function that runs before the end
func (cg *ConsumerGroup) OnClose(cb func()) {
	cg.onClose = append(cg.onClose, cb)
}

func (cg *ConsumerGroup) autoReconnect(interval time.Duration) {
	timer := time.NewTimer(interval)
	cg.logger.WithField("group", cg.name).Info("The auto-reconnect consumer thread was started")
	defer cg.logger.WithField("group", cg.name).Info("The auto-reconnect consumer thread was stopped")
	for {
		select {
		case <-cg.stopCh:
			return
		case <-timer.C:
			timer.Reset(interval)
			exist, err := cg.storage.existsConsumer(cg.name, cg.id)
			if err != nil {
				cg.logger.WithFields(logrus.Fields{
					"group": cg.name,
					"err":   err,
				}).Error("Failed to check consumer existence")
				break
			}
			if exist {
				break
			}
			err = cg.storage.registerConsumer(cg.name, cg.id, nil)
			if err != nil {
				cg.logger.WithFields(logrus.Fields{
					"group": cg.name,
					"err":   err,
				}).Error("Failed to re-register consumer")
			}
		}
	}
}

func (cg *ConsumerGroup) watchRebalance() error {
	consumersWatcher, err := cg.storage.watchConsumerList(cg.name)
	if err != nil {
		return err
	}
	topicsChange, topicWatchers, err := cg.watchTopics(cg.config.TopicList)
	if err != nil {
		return err
	}
	go func() {
		defer cg.callRecover()
		cg.logger.WithField("group", cg.name).Info("Rebalance watcher thread was started")
		select {
		case <-consumersWatcher.EvCh:
			cg.triggerRebalance()
			cg.logger.WithField("group", cg.name).Info("Trigger rebalance while consumers was changed")
			for _, tw := range topicWatchers {
				cg.storage.removeWatcher(tw)
			}
		case topic := <-topicsChange:
			cg.triggerRebalance()
			cg.logger.WithFields(logrus.Fields{
				"group": cg.name,
				"topic": topic,
			}).Info("Trigger rebalance while partitions was changed")
			cg.storage.removeWatcher(consumersWatcher)
		case <-cg.stopCh:
		}
		cg.logger.WithField("group", cg.name).Info("Rebalance watcher thread was exited")
	}()
	return nil
}

// CommitOffset is used to commit offset when auto commit was disabled.
func (cg *ConsumerGroup) CommitOffset(topic string, partition int32, offset int64) error {
	if cg.config.OffsetAutoCommitEnable {
		return errors.New("commit offset take effect when offset auto commit was disabled")
	}
	return cg.storage.commitOffset(cg.name, topic, partition, offset)
}

// GetOffsets return the offset in memory for debug
func (cg *ConsumerGroup) GetOffsets() map[string]interface{} {
	topics := make(map[string]interface{})
	for topic, tc := range cg.topicConsumers {
		topics[topic] = tc.getOffsets()
	}
	return topics
}

// Owners return owners of all partitions
func (cg *ConsumerGroup) Owners() map[string]map[int32]string {
	return cg.owners
}

func (cg *ConsumerGroup) watchTopics(topics []string) (<-chan string, []*zk.Watcher, error) {
	ch := make(chan string)
	cases := make([]reflect.SelectCase, len(topics))
	watchers := make([]*zk.Watcher, len(topics))
	for i, topic := range topics {
		w, err := cg.storage.watchTopic(topic)
		if err != nil {
			return nil, nil, fmt.Errorf("encounter error when watch topic: %s, err: %s", topic, err)
		}
		watchers[i] = w
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(w.EvCh)}
	}
	go func(cases []reflect.SelectCase, ch chan string, topics []string) {
		chosen, _, ok := reflect.Select(cases)
		if !ok {
			//the chosen channel has been closed.
			return
		}
		topic := topics[chosen]
		num, err := cg.storage.getPartitionsNum(topic)
		if err != nil {
			cg.logger.WithFields(logrus.Fields{
				"topic": topic,
				"err":   err,
			}).Error("Failed to get partitions in zookeeper after topic metadata change")
			return
		}
		for {
			saramaClient := cg.saramaClients[topic]
			saramaClient.RefreshMetadata(topic)
			partitions, err := saramaClient.Partitions(topic)
			if err != nil {
				cg.logger.WithFields(logrus.Fields{
					"topic": topic,
					"err":   err,
				}).Error("Failed to get partitions in broker after topic metadata change")
				return
			}
			if len(partitions) == num {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}

		ch <- topics[chosen]
	}(cases, ch, topics)
	return ch, watchers, nil
}
