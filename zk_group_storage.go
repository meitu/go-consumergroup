package consumergroup

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

// constants defining the fixed path format.
const (
	OWNER_PATH     = "/consumers/%s/owners/%s/%d"
	CONSUMERS_DIR  = "/consumers/%s/ids"
	CONSUMERS_PATH = "/consumers/%s/ids/%s"
	OFFSETS_PATH   = "/consumers/%s/offsets/%s/%d"
	BROKERS_DIR    = "/brokers/ids"
	BROKERS_PATH   = "/brokers/ids/%s"
)

// ZKGroupStorage is an instance of GroupStorage.
type ZKGroupStorage struct {
	serverList     []string
	client         *zk.Conn
	sessionTimeout time.Duration
}

var (
	ErrInvalidGroup      = errors.New("Invalid group")
	ErrInvalidTopic      = errors.New("Invalid topic")
	ErrInvalidConsumerID = errors.New("Invalid consumer ID")
	ErrInvalidPartition  = "Invalid partition %s"
)

// NewZKGroupStorage creates a new zookeeper group storage instance using the
// given server list and session timeout.
func NewZKGroupStorage(serverList []string, sessionTimeout time.Duration) (*ZKGroupStorage, error) {
	if len(serverList) == 0 {
		return nil, errors.New("zookeeper server list is invalid")
	}

	s := new(ZKGroupStorage)
	if sessionTimeout <= 0 {
		sessionTimeout = 6 * time.Second
	}
	s.serverList = serverList
	s.sessionTimeout = sessionTimeout
	return s, nil
}

// GetClient returns a zookeeper connetion.
func (s *ZKGroupStorage) GetClient() (*zk.Conn, error) {
	var err error
	if s.client == nil {
		s.client, _, err = zk.Connect(s.serverList, s.sessionTimeout)
	}
	return s.client, err
}

// CloseBadConn closes bad connections to prevent them from being used later.
func (s *ZKGroupStorage) CloseBadConn(err error) {
	if err == zk.ErrConnectionClosed || err == zk.ErrClosing || err == zk.ErrUnknown ||
		err == zk.ErrSessionExpired || err == zk.ErrSessionMoved {
		if s.client != nil {
			s.client.Close()
			s.client = nil
		}
	}
}

func (s *ZKGroupStorage) ClaimPartition(group, topic string, partition int32, consumerID string) error {
	if group == "" {
		return ErrInvalidGroup
	}
	if topic == "" {
		return ErrInvalidTopic
	}
	if consumerID == "" {
		return ErrInvalidConsumerID
	}
	if partition < 0 {
		return fmt.Errorf(ErrInvalidPartition, partition)
	}

	c, err := s.GetClient()
	if err != nil {
		return err
	}
	zkPath := fmt.Sprintf(OWNER_PATH, group, topic, partition)
	err = ZKCreateEphemeralPath(c, zkPath, []byte(consumerID))
	s.CloseBadConn(err)
	return err
}

func (s *ZKGroupStorage) ReleasePartition(group, topic string, partition int32) error {
	if group == "" {
		return ErrInvalidGroup
	}
	if topic == "" {
		return ErrInvalidTopic
	}
	if partition < 0 {
		return fmt.Errorf(ErrInvalidPartition, partition)
	}

	c, err := s.GetClient()
	if err != nil {
		return err
	}
	zkPath := fmt.Sprintf(OWNER_PATH, group, topic, partition)
	err = c.Delete(zkPath, -1)
	s.CloseBadConn(err)
	return err
}

func (s *ZKGroupStorage) GetPartitionOwner(group, topic string, partition int32) (string, error) {
	if group == "" {
		return "", ErrInvalidGroup
	}
	if topic == "" {
		return "", ErrInvalidTopic
	}
	if partition < 0 {
		return "", fmt.Errorf(ErrInvalidPartition, partition)
	}

	c, err := s.GetClient()
	if err != nil {
		return "", err
	}
	zkPath := fmt.Sprintf(OWNER_PATH, group, topic, partition)
	value, _, err := c.Get(zkPath)
	if err != nil {
		s.CloseBadConn(err)
		return "", err
	}
	return string(value), nil
}

func (s *ZKGroupStorage) RegisterConsumer(group, consumerID string, data []byte) error {
	if group == "" {
		return ErrInvalidGroup
	}
	if consumerID == "" {
		return ErrInvalidConsumerID
	}

	c, err := s.GetClient()
	if err != nil {
		return err
	}
	zkPath := fmt.Sprintf(CONSUMERS_PATH, group, consumerID)
	err = ZKCreateEphemeralPath(c, zkPath, data)
	s.CloseBadConn(err)
	return err
}

func (s *ZKGroupStorage) DeleteConsumer(group, consumerID string) error {
	if group == "" {
		return ErrInvalidGroup
	}
	if consumerID == "" {
		return ErrInvalidConsumerID
	}

	c, err := s.GetClient()
	if err != nil {
		return err
	}
	zkPath := fmt.Sprintf(CONSUMERS_PATH, group, consumerID)
	err = c.Delete(zkPath, -1)
	s.CloseBadConn(err)
	return err
}

func (s *ZKGroupStorage) WatchConsumerList(group string) (<-chan zk.Event, error) {
	if group == "" {
		return nil, ErrInvalidGroup
	}

	c, err := s.GetClient()
	if err != nil {
		return nil, err
	}

	zkPath := fmt.Sprintf(CONSUMERS_DIR, group)
	_, _, ech, err := c.ChildrenW(zkPath)
	if err != nil {
		s.CloseBadConn(err)
		return nil, err
	}
	return ech, nil
}

func (s *ZKGroupStorage) watchTopicChange(topic string) {
	// TODO;
}

func (s *ZKGroupStorage) GetBrokerList() ([]string, error) {
	var brokerList []string
	type broker struct {
		Host string
		Port int
	}
	var b broker

	c, err := s.GetClient()
	if err != nil {
		return nil, err
	}

	idList, _, err := c.Children(BROKERS_DIR)
	if err != nil {
		s.CloseBadConn(err)
		return nil, err
	}

	for _, id := range idList {
		zkPath := fmt.Sprintf(BROKERS_PATH, id)
		value, _, err := c.Get(zkPath)
		err = json.Unmarshal(value, &b)
		if err != nil {
			s.CloseBadConn(err)
			return nil, err
		}
		brokerList = append(brokerList, fmt.Sprintf("%s:%d", b.Host, b.Port))
	}
	return brokerList, nil
}

func (s *ZKGroupStorage) GetConsumerList(group string) ([]string, error) {
	if group == "" {
		return nil, ErrInvalidGroup
	}

	c, err := s.GetClient()
	if err != nil {
		return nil, err
	}

	zkPath := fmt.Sprintf(CONSUMERS_DIR, group)
	consumerList, _, err := c.Children(zkPath)
	if err != nil {
		s.CloseBadConn(err)
		return nil, err
	}
	return consumerList, err
}

func (s *ZKGroupStorage) CommitOffset(group, topic string, partition int32, offset int64) error {
	if group == "" {
		return ErrInvalidGroup
	}
	if topic == "" {
		return ErrInvalidTopic
	}
	if partition < 0 {
		return fmt.Errorf(ErrInvalidPartition, partition)
	}

	c, err := s.GetClient()
	if err != nil {
		return err
	}
	data := []byte(strconv.FormatInt(offset, 10))
	zkPath := fmt.Sprintf(OFFSETS_PATH, group, topic, partition)
	err = ZKSetPersistentPath(c, zkPath, data)
	s.CloseBadConn(err)
	return err
}

func (s *ZKGroupStorage) GetOffset(group, topic string, partition int32) (int64, error) {
	if group == "" {
		return -1, ErrInvalidGroup
	}
	if topic == "" {
		return -1, ErrInvalidTopic
	}
	if partition < 0 {
		return -1, fmt.Errorf(ErrInvalidPartition, partition)
	}

	c, err := s.GetClient()
	if err != nil {
		return -1, err
	}
	zkPath := fmt.Sprintf(OFFSETS_PATH, group, topic, partition)
	value, _, err := c.Get(zkPath)
	if err != nil {
		s.CloseBadConn(err)
		if err != zk.ErrNoNode {
			return -1, err
		}
		return 0, nil
	}
	return strconv.ParseInt(string(value), 10, 64)
}
