package kafka

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/songjiao/logzoom/input"
	"gopkg.in/yaml.v2"
	"os"
	"os/signal"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
	"gopkg.in/Shopify/sarama.v1"
	"github.com/songjiao/logzoom/buffer"
	"encoding/json"
)

var (
	zookeeperNodes []string
)

type Config struct {
	Zookeeper     string `yaml:"zookeeper"`
	Topic         string `yaml:"topic"`
	ConsumerGroup string `yaml:"consumer_group"`
}

type KafkaInput struct {
	name     string
	config   Config
	receiver input.Receiver
	term     chan bool
}

func init() {
	input.Register("kafka", New)
}

func New() input.Input {
	return &KafkaInput{term: make(chan bool, 1)}
}

func (kafkaServer *KafkaInput) ValidateConfig(config *Config) error {
	if len(config.Zookeeper) == 0 {
		return errors.New("Missing zookeeper")
	}

	if len(config.ConsumerGroup) == 0 {
		return errors.New("Missing consume_group")
	}

	if len(config.Topic) == 0 {
		return errors.New("Missing topic")
	}

	return nil
}

func (kafkaServer *KafkaInput) Init(name string, config yaml.MapSlice, receiver input.Receiver) error {
	var kafkaConfig *Config

	// go-yaml doesn't have a great way to partially unmarshal YAML data
	// See https://github.com/go-yaml/yaml/issues/13
	yamlConfig, _ := yaml.Marshal(config)

	if err := yaml.Unmarshal(yamlConfig, &kafkaConfig); err != nil {
		return fmt.Errorf("Error parsing kafka config: %v", err)
	}

	if err := kafkaServer.ValidateConfig(kafkaConfig); err != nil {
		return fmt.Errorf("Error in config: %v", err)
	}

	kafkaServer.name = name
	kafkaServer.config = *kafkaConfig
	kafkaServer.receiver = receiver

	return nil
}

func (kafkaServer *KafkaInput) Start() error {
	log.Printf("Starting kafkaV2 consumer")

	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(kafkaServer.config.Zookeeper)
	log.Printf("connnected to zk: [%s]", kafkaServer.config.Zookeeper)
	kafkaTopics := strings.Split(kafkaServer.config.Topic, ",")

	consumer, consumerErr := consumergroup.JoinConsumerGroup(kafkaServer.config.ConsumerGroup, kafkaTopics, zookeeperNodes, config)
	log.Printf("consume topic:%s comsume_group:%s", kafkaServer.config.Topic, kafkaServer.config.ConsumerGroup)
	if consumerErr != nil {
		log.Fatalln(consumerErr)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		if err := consumer.Close(); err != nil {
			sarama.Logger.Println("Error closing the consumer", err)
		}
	}()

	go func() {
		for err := range consumer.Errors() {
			log.Println(err)
		}
	}()

	eventCount := 0
	offsets := make(map[string]map[int32]int64)

	for message := range consumer.Messages() {
		if offsets[message.Topic] == nil {
			offsets[message.Topic] = make(map[int32]int64)
		}

		eventCount += 1
		if offsets[message.Topic][message.Partition] != 0 && offsets[message.Topic][message.Partition] != message.Offset - 1 {
			log.Printf("Unexpected offset on %s:%d. Expected %d, found %d, diff %d.\n", message.Topic, message.Partition, offsets[message.Topic][message.Partition] + 1, message.Offset, message.Offset - offsets[message.Topic][message.Partition] + 1)
		}
		line := string(message.Value)

		var ev buffer.Event
		ev.Text = &line
		var dat map[string]interface{}
		if err := json.Unmarshal(message.Value, &dat); err == nil {
			if val, ok := dat["id"]; ok {
				ev.ID = val.(string)
			}
			if val, ok := dat["type"]; ok {
				ev.Type = val.(string)
			}

		}

		kafkaServer.receiver.Send(&ev)
		offsets[message.Topic][message.Partition] = message.Offset
		consumer.CommitUpto(message)
	}

	for {
		select {
		case <-kafkaServer.term:
			log.Println("Kafka input server received term signal")
			return nil
		}
	}

	return nil
}

func (kafkaServer *KafkaInput) Stop() error {
	kafkaServer.term <- true
	return nil
}

