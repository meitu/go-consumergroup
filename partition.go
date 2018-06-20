package consumergroup

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type partitionConsumer struct {
	owner      *topicConsumer
	group      string
	topic      string
	partition  int32
	offset     int64
	prevOffset int64

	consumer sarama.PartitionConsumer
}

func newPartitionConsumer(owner *topicConsumer, partition int32) *partitionConsumer {
	return &partitionConsumer{
		owner:      owner,
		topic:      owner.name,
		group:      owner.owner.name,
		partition:  partition,
		offset:     0,
		prevOffset: 0,
	}
}

func (pc *partitionConsumer) start() {
	var wg sync.WaitGroup

	cg := pc.owner.owner
	err := pc.claim()
	if err != nil {
		cg.logger.Errorf("Failed to claim topic[%s] partition[%d] and give up, err %s",
			pc.topic, pc.partition, err)
		goto ERROR
	}
	defer func() {
		err = pc.release()
		if err != nil {
			cg.logger.Errorf("Failed to release topic[%s] partition[%d], err %s",
				pc.topic, pc.partition, err)
		} else {
			cg.logger.Infof("Release topic[%s] partition[%d] success", pc.topic, pc.partition)
		}
	}()

	err = pc.loadOffsetFromZk()
	if err != nil {
		cg.logger.Errorf("Failed to load topic[%s] partition[%d] offset from zk, err %s",
			pc.topic, pc.partition, err)
		goto ERROR
	}
	cg.logger.Debugf("Get topic[%s] partition[%d] offset[%d] from offset storage",
		pc.topic, pc.partition, pc.offset)

	pc.consumer, err = cg.getPartitionConsumer(pc.topic, pc.partition, pc.offset)
	if err != nil {
		cg.logger.Errorf("Failed to topic[%s] partition[%d] message, err %s",
			pc.topic, pc.partition, err)
		goto ERROR
	}
	defer pc.consumer.Close()

	if cg.config.OffsetAutoCommitEnable { // start auto commit-offset thread when enable
		wg.Add(1)
		go func() {
			defer cg.callRecover()
			defer wg.Done()
			cg.logger.Infof("Offset auto-commit topic[%s] partition[%d] thread was started",
				pc.topic, pc.partition)
			pc.autoCommitOffset()
		}()
	}

	pc.fetch()
	if cg.config.OffsetAutoCommitEnable {
		err = pc.commitOffset()
		if err != nil {
			cg.logger.Errorf("Failed to commit topic[%s] partition[%d] offset[%d]",
				pc.topic, pc.partition, pc.offset)
		}
		wg.Wait() // Wait for auto-commit-offset thread
		cg.logger.Infof("Offset auto-commit topic[%s] partition[%d] thread was stoped",
			pc.topic, pc.partition)
	}
	return

ERROR:
	cg.stop()
}

func (pc *partitionConsumer) loadOffsetFromZk() error {
	cg := pc.owner.owner
	offset, err := cg.storage.getOffset(pc.group, pc.topic, pc.partition)
	if err != nil {
		return err
	}
	if offset == -1 {
		offset = cg.config.OffsetAutoReset
	}
	pc.offset = offset
	pc.prevOffset = offset
	return nil
}

func (pc *partitionConsumer) claim() error {
	cg := pc.owner.owner
	timer := time.NewTimer(cg.config.ClaimPartitionRetryInterval)
	defer timer.Stop()
	retry := cg.config.ClaimPartitionRetryTimes
	// Claim partition would retry until success
	for i := 0; i < retry+1 || retry <= 0; i++ {
		err := cg.storage.claimPartition(pc.group, pc.topic, pc.partition, cg.id)
		if err == nil {
			return nil
		}
		if i%3 == 0 || retry > 0 {
			cg.logger.Errorf("Failed to claim topic[%s] partition[%d] after %d retires, err %s",
				pc.topic, pc.partition, i, err)
		}
		select {
		case <-timer.C:
			timer.Reset(cg.config.ClaimPartitionRetryInterval)
		case <-cg.stopper:
			return errors.New("stop signal was received when claim partition")
		}
	}
	return fmt.Errorf("claim partition err, after %d retries", retry)
}

func (pc *partitionConsumer) release() error {
	cg := pc.owner.owner
	owner, err := cg.storage.getPartitionOwner(pc.group, pc.topic, pc.partition)
	if err != nil {
		return err
	}
	if cg.id == owner {
		return cg.storage.releasePartition(pc.group, pc.topic, pc.partition)
	}
	return errors.New("partition wasn't ownered by this consumergroup")
}

func (pc *partitionConsumer) fetch() {
	cg := pc.owner.owner
	messageChan := pc.owner.messages
	errorChan := pc.owner.errors

PARTITION_CONSUMER_LOOP:
	for {
		select {
		case <-cg.stopper:
			break PARTITION_CONSUMER_LOOP
		case err := <-pc.consumer.Errors():
			errorChan <- err
		case message := <-pc.consumer.Messages():
			if message == nil {
				cg.logger.Errorf("Sarama partition consumer encounter error, the consumer would be exited")
				close(cg.stopper)
				break PARTITION_CONSUMER_LOOP
			}
			select {
			case messageChan <- message:
				pc.offset = message.Offset + 1
			case <-cg.stopper:
				break PARTITION_CONSUMER_LOOP
			}
		}
	}
}

func (pc *partitionConsumer) autoCommitOffset() {
	cg := pc.owner.owner
	defer cg.callRecover()
	timer := time.NewTimer(cg.config.OffsetAutoCommitInterval)
	for {
		select {
		case <-cg.stopper:
			return
		case <-timer.C:
			err := pc.commitOffset()
			if err != nil {
				cg.logger.Errorf("Failed to auto commit topic[%s] partition[%d] offset[%d], err :%s",
					pc.topic, pc.partition, pc.offset, err)
			}
			timer.Reset(cg.config.OffsetAutoCommitInterval)
		}
	}
}

func (pc *partitionConsumer) commitOffset() error {
	cg := pc.owner.owner
	offset := pc.offset
	if pc.prevOffset == offset {
		return nil
	}
	err := cg.storage.commitOffset(pc.group, pc.topic, pc.partition, offset)
	if err != nil {
		return err
	}
	pc.prevOffset = offset
	return nil
}
