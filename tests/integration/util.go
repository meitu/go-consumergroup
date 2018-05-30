package main

import (
	"fmt"
	"math/rand"
	"time"

	consumergroup "github.com/meitu/go-consumergroup"

	"github.com/Shopify/sarama"
)

func genRandomGroupID(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	suffix := ""
	for i := 0; i < n; i++ {
		suffix = suffix + string(letterBytes[rand.Int()%len(letterBytes)])
	}
	return "integration-test-group-id-" + suffix
}

func produceMessages(addrs []string, topic string, partition int32, count int) {
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.Partitioner = sarama.NewManualPartitioner
	client, _ := sarama.NewClient(addrs, conf)
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		panic(fmt.Sprintf("Failed to create producer, err %s", err))
	}
	for i := 0; i < count; i++ {
		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Partition: partition,
			Value:     sarama.StringEncoder("test-value"),
		})
		if err != nil {
			panic(fmt.Sprintf("Failed to send message, err %s", err))
		}
	}
}

func createConsumerInstance(addrs []string, groupID, topic string) (*consumergroup.ConsumerGroup, error) {
	conf := consumergroup.NewConfig()
	conf.ZkList = addrs
	conf.ZkSessionTimeout = 6 * time.Second
	conf.TopicList = []string{topic}
	conf.GroupID = groupID
	conf.OffsetAutoCommitInterval = 100 * time.Millisecond

	cg, err := consumergroup.NewConsumerGroup(conf)
	if err != nil {
		return nil, err
	}
	err = cg.JoinGroup()
	if err != nil {
		return nil, err
	}
	return cg, nil
}
