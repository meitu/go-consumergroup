# go-consumergroup [![Build Status](https://travis-ci.org/meitu/go-consumergroup.svg?branch=master)](https://travis-ci.org/meitu/go-consumergroup)

Go-consumergroup is a kafka consumer library written in golang with group and rebalance supports.

[Chinese Doc](./README.zh-CN.md)

## Requirements
* Apache Kafka 0.8.x, 0.9.x, 0.10.x

## Dependencies
* [go-zookeeper](https://github.com/samuel/go-zookeeper)

* [sarama](https://github.com/Shopify/sarama)

## API
``` golang
func NewConfig(groupId string, topicList []string) (*Config, error)
```
creates a new config instance，most items in this config can be modified directly.

more details [Config](#config)

---

``` golang
func NewZKGroupStorage(serverList []string, sessionTimeout int) (*ZKGroupStorage, error) 
```

creates a group storage instance based on a zookeeper quorum

---

``` golang
func NewConsumerGroup(storage GroupStorage, config *Config) (*ConsumerGroup, error)
```

creates a consumer client with storage and config

---

``` golang
func (cg *ConsumerGroup) JoinGroup() error
```

joins a group and starts to consume

---

``` golang
func (cg *ConsumerGroup) ExitGroup()
```

stops consuming and exit

---

``` golang
func (cg *ConsumerGroup) GetTopicNextMessageChannel(topic string) (<-chan *sarama.ConsumerMessage, error) 
```

returns an unbuffered channel from which to get messages of a specified topic

---

``` golang 
func (cg *ConsumerGroup) GetTopicErrorsChannel(topic string) (<-chan *sarama.ConsumerError, error) 
```

returns a buffered channel from which to get error messages of a specified topic

---

``` golang
func (cg *ConsumerGroup) SetLogger(logger Logger)
```

sets the logger， you need to implement the Logger interface

more details [Logger](#logger)

---

more details about usage see [example](example/example.go)

### TYPE
#### Config 

``` golang
type Config struct {
	groupId    string
	topicList  []string

	SaramaConfig *sarama.Config 
	//sarama's config

	ErrorChannelBufferSize int 
	//size of the error channel buffer, defaults to 1024

	OffsetAutoCommitInterval time.Duration
	//offset auto commit interval, defaults to 10 seconds

	OffsetAutoReset          int64
	//sarama.OffsetNewest | sarama.OffsetOldest, defaults to sarama.OffsetNewest
	//fetch messages from the oldest or the lastest when the offset is not present in zookeeper or out of range

	ClaimPartitionRetry         int
	//maximum retries of claiming partitions, defaults to 5

	ClaimPartitionRetryInterval time.Duration
	//retry interval of claiming partitions, defaults to 3 seconds
}
```

#### Logger

``` golang
type Logger interface {
	Debug(args ...interface{})
	Debugf(format string, args ...interface{})
	Info(args ...interface{})
	Infof(format string, args ...interface{})
	Warn(args ...interface{})
	Warnf(format string, args ...interface{})
	Error(args ...interface{})
	Errorf(format string, args ...interface{})
}
```
