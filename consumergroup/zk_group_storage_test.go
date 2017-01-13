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
	testConsumerId = "go_test_consumer_id"
)

func TestZKGroupStorageInvalidServerList(t *testing.T) {
	_, err := NewZKGroupStorage(nil, 6*time.Second)
	if err == nil || err.Error() != "zookeeper server list is invalid" {
		t.Fatal("Expected zookeeper server list is invalid, got ", err)
	}
}

func TestZKGroupStorageEmptyServerList(t *testing.T) {
	_, err := NewZKGroupStorage([]string{}, 6*time.Second)
	if err == nil || err.Error() != "zookeeper server list is invalid" {
		t.Fatal("Expected zookeeper server list is invalid, got ", err)
	}
}

func TestZKGroupStorageValidates(t *testing.T) {
	_, err := NewZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)
	if err != nil {
		t.Fatal(err)
	}
}

func TestZKGroupStorageClaimAndGetAndReleasePartition(t *testing.T) {
	zk, _ := NewZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)

	err := zk.ClaimPartition(testGroup, testTopic, 0, testConsumerId)
	if err != nil {
		t.Error(err)
	}

	err = zk.ReleasePartition(testGroup, testTopic, 0)
	if err != nil {
		t.Error(err)
	}

	zk.ClaimPartition(testGroup, testTopic, 0, testConsumerId)
	err = zk.ClaimPartition(testGroup, testTopic, 0, testConsumerId)
	if err == nil {
		zk.ReleasePartition(testGroup, testTopic, 0)
		t.Error("Expected it can't claim a partition twice, but it did")
	}

	cid, err := zk.GetPartitionOwner(testGroup, testTopic, 0)
	if err != nil {
		zk.ReleasePartition(testGroup, testTopic, 0)
		t.Error("get partition owner failed, because: ", err)
	}
	if cid != testConsumerId {
		zk.ReleasePartition(testGroup, testTopic, 0)
		t.Error("partition owner get from zookeeper isn't expection")
	}

	zk.ReleasePartition(testGroup, testTopic, 0)
}

func TestZKGroupStorageRegisterAndGetAndDeleteConsumer(t *testing.T) {
	zk, _ := NewZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)

	err := zk.RegisterConsumer(testGroup, testConsumerId, nil)
	if err != nil {
		t.Fatal(err)
	}

	err = zk.DeleteConsumer(testGroup, testConsumerId)
	if err != nil {
		t.Fatal(err)
	}

	zk.RegisterConsumer(testGroup, testConsumerId, nil)
	err = zk.RegisterConsumer(testGroup, testConsumerId, nil)
	if err == nil {
		zk.DeleteConsumer(testGroup, testConsumerId)
		t.Fatal("Expected it can't register consumer twice, but it did")
	}

	consumerList, err := zk.GetConsumerList(testGroup)
	if err != nil {
		t.Fatal(err)
	}

	if consumerList[0] != testConsumerId {
		zk.DeleteConsumer(testGroup, testConsumerId)
		t.Fatal("consumer id get from zookeeper isn't expection")
	}
	zk.DeleteConsumer(testGroup, testConsumerId)
}

func TestZKGroupWatchConsumerList(t *testing.T) {
	zk, _ := NewZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)

	consumer1 := fmt.Sprintf(testConsumerId, rand.Int())
	consumer2 := fmt.Sprintf(testConsumerId, rand.Int())
	consumer3 := fmt.Sprintf(testConsumerId, rand.Int())
	consumerList := []string{consumer1, consumer2, consumer3}
	for _, consumer := range consumerList {
		zk.RegisterConsumer(testGroup, consumer, nil)
	}

	ch, err := zk.WatchConsumerList(testGroup)
	if err != nil {
		t.Error(err)
	}

	select {
	case <-ch:
		t.Error("channel receive message before consumer list change")
	default:
	}

	zk.DeleteConsumer(testGroup, consumer1)

	select {
	case <-ch:
	default:
		t.Error("channel can't receive message after consumer list change")
	}

	for _, consumer := range consumerList {
		zk.DeleteConsumer(testGroup, consumer)
	}
}

func TestZKGroupStorageCommitAndGetOffset(t *testing.T) {
	zk, _ := NewZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)
	testOffset := rand.Int63()

	err := zk.CommitOffset(testGroup, testTopic, 0, testOffset)
	if err != nil {
		t.Error(err)
	}

	offset, err := zk.GetOffset(testGroup, testTopic, 0)
	if err != nil {
		t.Error(err)
	}

	if offset != testOffset {
		t.Error("offset get from zookeeper isn't expection")
	}

	err = zk.CommitOffset(testGroup, testTopic, 0, testOffset+1)
	if err != nil {
		t.Error(err)
	}
}
