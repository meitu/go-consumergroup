package consumergroup

import "testing"

func TestConfigValidate(t *testing.T) {
	conf := NewConfig()
	conf.ZkList = []string{}
	conf.TopicList = []string{}
	if err := conf.validate(); err == nil {
		t.Fatal("config invalidate is expected")
	}
	conf.ZkList = []string{"127.0.0.1:2181", "127.0.0.1:2181"}
	if err := conf.validate(); err == nil {
		t.Fatal("config invalidate is expected")
	}
	conf.TopicList = []string{"a", "a", "b", "c", "a"}
	if err := conf.validate(); err == nil {
		t.Fatal("config validate is expected")
	}
	conf.GroupID = "go-test-group"
	if err := conf.validate(); err == nil {
		t.Fatal("config invalidate is expected")
	}
	if len(conf.TopicList) != 3 {
		t.Fatal("config validate should remove duplicate topics")
	}
	if len(conf.ZkList) != 1 {
		t.Fatal("config validate should remove duplicate zk addresses")
	}
	if err := conf.validate(); err != nil {
		t.Fatalf("validate is expected, but got error %s", err)
	}
}
