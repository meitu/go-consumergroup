package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/meitu/go-consumergroup"
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

func registSignal(cg *consumergroup.ConsumerGroup) {
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
	topic := "test"
	topicList := []string{topic}
	config, err := consumergroup.NewConfig("go-test-group", topicList)
	if err != nil {
		fmt.Println("create a new config failed:", err.Error())
		os.Exit(1)
	}

	storage, _ := consumergroup.NewZKGroupStorage([]string{"127.0.0.1:2181"}, 6*time.Second)

	cg, err := consumergroup.NewConsumerGroup(storage, config)
	if err != nil {
		fmt.Println("create a new consumer failed:", err.Error())
		os.Exit(1)
	}

	registSignal(cg)

	err = cg.JoinGroup()
	if err != nil {
		fmt.Println("join group failed: ", err.Error())
		os.Exit(1)
	}

	messages, err := cg.GetTopicNextMessageChannel(topic)
	if err != nil {
		fmt.Println("get message channel failed: ", err.Error())
	}

	for message := range messages {
		fmt.Println(string(message.Value), message.Offset)
		time.Sleep(500 * time.Millisecond)
	}
}
