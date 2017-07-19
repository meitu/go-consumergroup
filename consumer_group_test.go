package consumergroup

import (
	"fmt"
	"testing"
	"time"
)

func TestConsumerGroup(t *testing.T) {
	conf := NewConfig()
	conf.TopicList = []string{"go-test-topic"}
	conf.GroupID = "go-test-group"
	conf.ZkList = []string{"127.0.0.1:2181"}
	conf.ZkSessionTimeout = 6 * time.Second
	cg, err := NewConsumerGroup(conf)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(cg.JoinGroup())
}
