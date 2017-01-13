package consumergroup

import (
	"fmt"
	"testing"
	"time"
)

func TestConsumerGroup(t *testing.T) {
	storage, _ := NewZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)
	topicList := []string{"go-test-topic"}
	config, err := NewConfig("go-test-group", topicList)
	cg, err := NewConsumerGroup(storage, config)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(cg.JoinGroup())
}
