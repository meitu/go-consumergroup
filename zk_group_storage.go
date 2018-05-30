package consumergroup

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/meitu/zk_wrapper"
	"github.com/samuel/go-zookeeper/zk"
)

// constants defining the fixed path format.
const (
	ownerPath     = "/consumers/%s/owners/%s/%d"
	consumersDir  = "/consumers/%s/ids"
	consumersPath = "/consumers/%s/ids/%s"
	offsetsPath   = "/consumers/%s/offsets/%s/%d"
	brokersDir    = "/brokers/ids"
	brokersPath   = "/brokers/ids/%s"
)

type zkGroupStorage struct {
	chroot         string
	serverList     []string
	client         *zk_wrapper.Conn
	sessionTimeout time.Duration
}

var (
	errInvalidGroup      = errors.New("Invalid group")
	errInvalidTopic      = errors.New("Invalid topic")
	errInvalidConsumerID = errors.New("Invalid consumer ID")
	errInvalidPartition  = "Invalid partition %s"
)

func newZKGroupStorage(serverList []string, sessionTimeout time.Duration) *zkGroupStorage {
	s := new(zkGroupStorage)
	if sessionTimeout <= 0 {
		sessionTimeout = 6 * time.Second
	}
	s.serverList = serverList
	s.sessionTimeout = sessionTimeout
	return s
}

func (s *zkGroupStorage) Chroot(chroot string) {
	s.chroot = chroot
}

// getClient returns a zookeeper connetion.
func (s *zkGroupStorage) getClient() (*zk_wrapper.Conn, error) {
	var err error
	if s.client == nil {
		s.client, _, err = zk_wrapper.Connect(s.serverList, s.sessionTimeout)
		if s.client != nil && s.chroot != "" {
			if err = s.client.Chroot(s.chroot); err != nil {
				return nil, err
			}
		}
	}
	return s.client, err
}

func (s *zkGroupStorage) claimPartition(group, topic string, partition int32, consumerID string) error {
	if group == "" {
		return errInvalidGroup
	}
	if topic == "" {
		return errInvalidTopic
	}
	if consumerID == "" {
		return errInvalidConsumerID
	}
	if partition < 0 {
		return fmt.Errorf(errInvalidPartition, partition)
	}

	c, err := s.getClient()
	if err != nil {
		return err
	}
	zkPath := fmt.Sprintf(ownerPath, group, topic, partition)
	err = zkCreateEphemeralPath(c, zkPath, []byte(consumerID))
	return err
}

func (s *zkGroupStorage) releasePartition(group, topic string, partition int32) error {
	if group == "" {
		return errInvalidGroup
	}
	if topic == "" {
		return errInvalidTopic
	}
	if partition < 0 {
		return fmt.Errorf(errInvalidPartition, partition)
	}

	c, err := s.getClient()
	if err != nil {
		return err
	}
	zkPath := fmt.Sprintf(ownerPath, group, topic, partition)
	err = c.Delete(zkPath, -1)
	return err
}

func (s *zkGroupStorage) getPartitionOwner(group, topic string, partition int32) (string, error) {
	if group == "" {
		return "", errInvalidGroup
	}
	if topic == "" {
		return "", errInvalidTopic
	}
	if partition < 0 {
		return "", fmt.Errorf(errInvalidPartition, partition)
	}

	c, err := s.getClient()
	if err != nil {
		return "", err
	}
	zkPath := fmt.Sprintf(ownerPath, group, topic, partition)
	value, _, err := c.Get(zkPath)
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (s *zkGroupStorage) registerConsumer(group, consumerID string, data []byte) error {
	if group == "" {
		return errInvalidGroup
	}
	if consumerID == "" {
		return errInvalidConsumerID
	}

	c, err := s.getClient()
	if err != nil {
		return err
	}
	zkPath := fmt.Sprintf(consumersPath, group, consumerID)
	err = zkCreateEphemeralPath(c, zkPath, data)
	return err
}

func (s *zkGroupStorage) existsConsumer(group, consumerID string) (bool, error) {
	if group == "" {
		return false, errInvalidGroup
	}
	if consumerID == "" {
		return false, errInvalidConsumerID
	}

	c, err := s.getClient()
	if err != nil {
		return false, err
	}
	zkPath := fmt.Sprintf(consumersPath, group, consumerID)
	exist, _, err := c.Exists(zkPath)
	return exist, err
}

func (s *zkGroupStorage) deleteConsumer(group, consumerID string) error {
	if group == "" {
		return errInvalidGroup
	}
	if consumerID == "" {
		return errInvalidConsumerID
	}

	c, err := s.getClient()
	if err != nil {
		return err
	}
	zkPath := fmt.Sprintf(consumersPath, group, consumerID)
	err = c.Delete(zkPath, -1)
	return err
}

func (s *zkGroupStorage) watchConsumerList(group string) (<-chan zk.Event, error) {
	if group == "" {
		return nil, errInvalidGroup
	}

	c, err := s.getClient()
	if err != nil {
		return nil, err
	}

	zkPath := fmt.Sprintf(consumersDir, group)
	_, _, ech, err := c.ChildrenW(zkPath)
	if err != nil {
		return nil, err
	}
	return ech, nil
}

func (s *zkGroupStorage) watchTopicChange(topic string) {
	// TODO;
}

func (s *zkGroupStorage) getBrokerList() ([]string, error) {
	var brokerList []string
	type broker struct {
		Host string
		Port int
	}
	var b broker

	c, err := s.getClient()
	if err != nil {
		return nil, err
	}

	idList, _, err := c.Children(brokersDir)
	if err != nil {
		return nil, err
	}

	for _, id := range idList {
		zkPath := fmt.Sprintf(brokersPath, id)
		value, _, err := c.Get(zkPath)
		err = json.Unmarshal(value, &b)
		if err != nil {
			return nil, err
		}
		brokerList = append(brokerList, fmt.Sprintf("%s:%d", b.Host, b.Port))
	}
	return brokerList, nil
}

func (s *zkGroupStorage) getConsumerList(group string) ([]string, error) {
	if group == "" {
		return nil, errInvalidGroup
	}

	c, err := s.getClient()
	if err != nil {
		return nil, err
	}

	zkPath := fmt.Sprintf(consumersDir, group)
	consumerList, _, err := c.Children(zkPath)
	if err != nil {
		return nil, err
	}
	sort.Strings(consumerList)
	return consumerList, nil
}

func (s *zkGroupStorage) commitOffset(group, topic string, partition int32, offset int64) error {
	if group == "" {
		return errInvalidGroup
	}
	if topic == "" {
		return errInvalidTopic
	}
	if partition < 0 {
		return fmt.Errorf(errInvalidPartition, partition)
	}

	c, err := s.getClient()
	if err != nil {
		return err
	}
	data := []byte(strconv.FormatInt(offset, 10))
	zkPath := fmt.Sprintf(offsetsPath, group, topic, partition)
	err = zkSetPersistentPath(c, zkPath, data)
	return err
}

func (s *zkGroupStorage) getOffset(group, topic string, partition int32) (int64, error) {
	if group == "" {
		return -1, errInvalidGroup
	}
	if topic == "" {
		return -1, errInvalidTopic
	}
	if partition < 0 {
		return -1, fmt.Errorf(errInvalidPartition, partition)
	}

	c, err := s.getClient()
	if err != nil {
		return -1, err
	}
	zkPath := fmt.Sprintf(offsetsPath, group, topic, partition)
	value, _, err := c.Get(zkPath)
	if err != nil {
		if err != zk.ErrNoNode {
			return -1, err
		}
		return -1, nil
	}
	return strconv.ParseInt(string(value), 10, 64)
}
