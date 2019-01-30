package consumergroup

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

const (
	testValue      = "go_test_value"
	testTopic      = "go_test_topic"
	testGroup      = "go_test_group"
	testConsumerID = "go_test_consumer_id"
)

func TestZKGroupStorageClaimAndGetAndReleasePartition(t *testing.T) {
	zk := newZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)

	err := zk.claimPartition(testGroup, testTopic, 0, testConsumerID)
	if err != nil {
		t.Error(err)
	}

	err = zk.releasePartition(testGroup, testTopic, 0)
	if err != nil {
		t.Error(err)
	}

	zk.claimPartition(testGroup, testTopic, 0, testConsumerID)
	err = zk.claimPartition(testGroup, testTopic, 0, testConsumerID)
	if err == nil {
		zk.releasePartition(testGroup, testTopic, 0)
		t.Error("Expected it can't claim a partition twice, but it did")
	}

	cid, err := zk.getPartitionOwner(testGroup, testTopic, 0)
	if err != nil {
		zk.releasePartition(testGroup, testTopic, 0)
		t.Error("get partition owner failed, because: ", err)
	}
	if cid != testConsumerID {
		zk.releasePartition(testGroup, testTopic, 0)
		t.Error("partition owner get from zookeeper isn't unexpected")
	}

	zk.releasePartition(testGroup, testTopic, 0)
}

func TestZKGroupStorageRegisterAndGetAndDeleteConsumer(t *testing.T) {
	zk := newZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)

	err := zk.registerConsumer(testGroup, testConsumerID, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = zk.deleteConsumer(testGroup, testConsumerID)
	if err != nil {
		t.Fatal(err)
	}

	zk.registerConsumer(testGroup, testConsumerID, nil)
	err = zk.registerConsumer(testGroup, testConsumerID, nil)
	if err == nil {
		zk.deleteConsumer(testGroup, testConsumerID)
		t.Fatal("Expected it can't register consumer twice, but it did")
	}

	consumerList, err := zk.getConsumerList(testGroup)
	if err != nil {
		t.Fatal(err)
	}

	if consumerList[0] != testConsumerID {
		zk.deleteConsumer(testGroup, testConsumerID)
		t.Fatal("consumer id get from zookeeper isn't expected")
	}
	zk.deleteConsumer(testGroup, testConsumerID)
}

func TestZKGroupWatchConsumerList(t *testing.T) {
	zk := newZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)

	consumer1 := fmt.Sprintf("%s-%d", testConsumerID, rand.Int())
	consumer2 := fmt.Sprintf("%s-%d", testConsumerID, rand.Int())
	consumer3 := fmt.Sprintf("%s-%d", testConsumerID, rand.Int())
	consumerList := []string{consumer1, consumer2, consumer3}
	for _, consumer := range consumerList {
		zk.registerConsumer(testGroup, consumer, nil)
	}

	watcher, err := zk.watchConsumerList(testGroup)
	if err != nil {
		t.Error(err)
	}

	select {
	case <-watcher.EvCh:
		t.Error("channel receive message before consumer list change")
	default:
	}

	zk.deleteConsumer(testGroup, consumer1)

	select {
	case <-watcher.EvCh:
	default:
		t.Error("channel can't receive message after consumer list change")
	}

	for _, consumer := range consumerList {
		zk.deleteConsumer(testGroup, consumer)
	}
}

func TestZKGroupStorageCommitAndGetOffset(t *testing.T) {
	zk := newZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)
	testOffset := rand.Int63()

	err := zk.commitOffset(testGroup, testTopic, 0, testOffset)
	if err != nil {
		t.Error(err)
	}

	offset, err := zk.getOffset(testGroup, testTopic, 0)
	if err != nil {
		t.Error(err)
	}

	if offset != testOffset {
		t.Error("offset get from zookeeper isn't unexpected")
	}

	err = zk.commitOffset(testGroup, testTopic, 0, testOffset+1)
	if err != nil {
		t.Error(err)
	}
}
