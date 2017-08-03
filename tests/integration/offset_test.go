package main

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

func TestOffsetAutoCommit(t *testing.T) {
	count := 10
	group := genRandomGroupID(10)
	c, err := createConsumerInstance(zookeepers, group, topic)
	if err != nil {
		t.Fatalf("Failed to create consumer instance, err %s", err)
	}
	defer c.ExitGroup()
	time.Sleep(3000 * time.Millisecond) // we have no way to know if the consumer is ready
	produceMessages(brokers, topic, 0, count)
	messages, _ := c.GetMessages(topic)
	go func(mgsCh <-chan *sarama.ConsumerMessage) {
		for message := range messages {
			fmt.Println(message)
		}
	}(messages)
	time.Sleep(200 * time.Millisecond) // offset auto commit interval is 100ms

	zkCli, _, err := zk.Connect(zookeepers, 6*time.Second)
	if err != nil {
		t.Fatal("Failed to connect zookeeper")
	}
	offsetPath := fmt.Sprintf("/consumers/%s/offsets/%s/%d", group, topic, 0)
	data, _, err := zkCli.Get(offsetPath)
	if err != nil {
		t.Fatalf("Failed to get partition offset, err %s", err)
	}
	offset, _ := strconv.Atoi(string(data))
	if offset+1 != count {
		t.Errorf("Auto commit offset expect %d, but got %d", count, offset)
	}
}
