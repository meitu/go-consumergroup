package consumergroup

import "testing"

var groupId string = "test_group"
var topicList []string = []string{"test-topic"}

func TestConfigInvalidGroupId(t *testing.T) {
	_, err := NewConfig("", topicList)
	if err == nil || err.Error() != "group id is invalid" {
		t.Error("Expected group id is invalid, got ", err)
	}
}

func TestConfigInvalidTopicList(t *testing.T) {
	_, err := NewConfig(groupId, nil)
	if err == nil || err.Error() != "topic list is invalid" {
		t.Error("Expected topic list is invalid, got ", err)
	}
}

func TestConfigEmptyTopicList(t *testing.T) {
	_, err := NewConfig(groupId, []string{})
	if err == nil || err.Error() != "topic list is invalid" {
		t.Error("Expected topic list is invalid, got ", err)
	}
}

func TestConfigValidates(t *testing.T) {
	_, err := NewConfig(groupId, topicList)
	if err != nil {
		t.Error(err)
	}
}
