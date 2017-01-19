package consumergroup

import "testing"

var groupID = "test_group"
var topicList = []string{"test-topic"}

func TestConfigInvalidGroupID(t *testing.T) {
	_, err := NewConfig("", topicList)
	if err == nil || err.Error() != "group id is invalid" {
		t.Error("Expected group id is invalid, got ", err)
	}
}

func TestConfigInvalidTopicList(t *testing.T) {
	_, err := NewConfig(groupID, nil)
	if err == nil || err.Error() != "topic list is invalid" {
		t.Error("Expected topic list is invalid, got ", err)
	}
}

func TestConfigEmptyTopicList(t *testing.T) {
	_, err := NewConfig(groupID, []string{})
	if err == nil || err.Error() != "topic list is invalid" {
		t.Error("Expected topic list is invalid, got ", err)
	}
}

func TestConfigValidates(t *testing.T) {
	_, err := NewConfig(groupID, topicList)
	if err != nil {
		t.Error(err)
	}
}
