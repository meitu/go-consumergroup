package main

import (
	"fmt"
	consumergroup "go-consumergroup"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func handleSignal(sig os.Signal, cg *consumergroup.ConsumerGroup) {
	switch sig {
	case syscall.SIGINT:
		cg.ExitGroup()
	case syscall.SIGTERM:
		cg.ExitGroup()
	default:
	}
}

func registerSignal(cg *consumergroup.ConsumerGroup) {
	go func() {
		c := make(chan os.Signal)
		sigs := []os.Signal{
			syscall.SIGINT,
			syscall.SIGTERM,
		}
		signal.Notify(c, sigs...)
		sig := <-c
		handleSignal(sig, cg)
	}()
}

func main() {
	conf := consumergroup.NewConfig()
	conf.ZkList = []string{"127.0.0.1:2181"}
	conf.ZkSessionTimeout = 6 * time.Second
	conf.TopicList = []string{"test"}
	conf.GroupID = "go-test-group-id"

	cg, err := consumergroup.NewConsumerGroup(conf)
	if err != nil {
		fmt.Println("Failed to create consumer group, err ", err.Error())
		os.Exit(1)
	}

	registerSignal(cg)

	err = cg.JoinGroup()
	if err != nil {
		fmt.Println("Failed to join group, err ", err.Error())
		os.Exit(1)
	}
	messages, err := cg.GetTopicNextMessageChannel("test")
	if err != nil {
		fmt.Println("Failed to get message channel, err", err.Error())
	}
	for message := range messages {
		fmt.Println(string(message.Value), message.Offset)
		time.Sleep(500 * time.Millisecond)
	}
}
