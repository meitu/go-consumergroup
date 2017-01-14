# go-consumergroup

### 简介
go-consumergroup是一款提供集群功能的kafka客户端，程序的实现是对sarama进行了一层包装，比其多提供了rebalance功能和自动管理offset的功能。目前基于zookeeper的集群功能已经开发完毕（适用于0.8及以后所有版本），基于kafka的集群功能还在开发中（适用于0.9.0.0及以上版本）。

### 依赖
<https://github.com/samuel/go-zookeeper>

<https://github.com/Shopify/sarama>

### API
``` golang
func NewConfig(groupId string, topicList []string) (*Config, error)
```
创建一个新的config，config里面的大部分配置项可以直接修改，详见[Config](#config)

---

``` golang
func NewZKGroupStorage(serverList []string, sessionTimeout int) (*ZKGroupStorage, error) 
```

创建一个基于zookeeper的GroupStorage，适用于kafka 0.8及之后的版本。

---

``` golang
func NewConsumerGroup(storage GroupStorage, config *Config) (*ConsumerGroup, error)
```

创建一个consumer客户端

---

``` golang
func (cg *ConsumerGroup) JoinGroup() error
```

加入集群中，并开始消费

---

``` golang
func (cg *ConsumerGroup) ExitGroup()
```

停止消费，并退出集群

---

``` golang
func (cg *ConsumerGroup) GetTopicNextMessageChannel(topic string) (<-chan *sarama.ConsumerMessage, error) 
```

获得一个无缓冲的channel，channel的传输的内容为该consumer从指定topic获取的消息

---

``` golang 
func (cg *ConsumerGroup) GetTopicErrorsChannel(topic string) (<-chan *sarama.ConsumerError, error) 
```

获得一个有缓冲的channel，channel传输的内容为该consumer从指定topic获得的error信息

---

``` golang
func (cg *ConsumerGroup) SetLogger(logger Logger)
```

设置一个日志打印函数，需要实现Logger接口，详见[Logger](#logger)

---

更多详细信息可以见同级目录下的[example](example/example.go)

### TYPE
#### Config 

``` golang
type Config struct {
	groupId    string
	topicList  []string
	
	SaramaConfig *sarama.Config 
	//sarama的config

	ErrorChannelBufferSize int 
	//每个topic的error channel的缓存大小，默认为1024
	
	OffsetAutoCommitInterval time.Duration
	//offset 自动提交的间隔时间，默认值为10秒
	OffsetAutoReset          int64  
	//offset auto offset时的策略，可选项有sarama.OffsetNewest和sarama.OffsetOldest，
	//默认值为sarama.OffsetNewest

	ClaimPartitionRetry         int
	//占用partition的最大重试次数，默认为5
	ClaimPartitionRetryInterval time.Duration
	//占用partition的重试间隔，默认值为3秒
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




