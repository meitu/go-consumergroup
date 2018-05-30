package main

import (
	"fmt"
	"testing"
	"time"

	consumergroup "github.com/meitu/go-consumergroup"

	"github.com/Shopify/sarama"
	"github.com/meitu/zk_wrapper"
)

func TestRebalance(t *testing.T) {
	group := genRandomGroupID(10)
	consumers := make([]*consumergroup.ConsumerGroup, 0)
	for i := 0; i < 3; i++ {
		go func() {
			c, err := createConsumerInstance(zookeepers, group, topic)
			if err != nil {
				t.Errorf("Failed to create consumer instance, err %s", err)
			}
			consumers = append(consumers, c)
		}()
	}
	time.Sleep(3 * time.Second) // we have no way to know if the consumer is ready

	kafkaCli, _ := sarama.NewClient(brokers, nil)
	partitions, err := kafkaCli.Partitions(topic)
	if err != nil {
		t.Errorf("Failed to get partitons, err %s", err)
	}
	owners := make([]string, 0)
	zkCli, _, err := zk_wrapper.Connect(zookeepers, 6*time.Second)
	if err != nil {
		t.Fatal("Failed to connect zookeeper")
	}
	for i := 0; i < len(partitions); i++ {
		ownerPath := fmt.Sprintf("/consumers/%s/owners/%s/%d", group, topic, i)
		data, _, err := zkCli.Get(ownerPath)
		if err != nil {
			t.Errorf("Failed to get partition owner, err %s", err)
		}
		owners = append(owners, string(data))
	}
	if len(owners) != len(partitions) {
		t.Errorf("Missing owner in some partitions expected %d, but got %d",
			len(partitions), len(owners))
	}
	for i := 1; i < len(owners); i++ {
		if owners[i] == owners[i-1] {
			t.Fatal("Partition owner should be difference while consumer > 1")
		}
	}

	for _, c := range consumers {
		c.ExitGroup()
	}
}
