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
	"strconv"
	"encoding/json"
)


var (
	zookeeperNodes []string
)

type NginxLog struct {
	Hostname               string
	Remote_addr            string
	Remote_user            string
	Time_local             string
	Request_time           string
	Content_length         string
	Gzip_ratio             string
	Request                string
	Status                 string
	Body_bytes_sent        string
	Http_referer           string
	Http_user_agent        string
	Host                   string
	Http_x_forwarded_for   string
	Upstream_addr          string
	Http_accept_language   string
	Nginxtype              string
	Upstream_response_time string
	Kslogid                string
	Msec                   string
	Request_length         string
	Bytes_sent             string
	Scheme                 string
}

type Config struct {
	Zookeeper       string `yaml:"zookeeper"`
	Topic           string `yaml:"topic"`
	ConsumerGroup   string `yaml:"consumer_group"`
	SeparatorChar   string `yaml:"separator_char"`
	Fields          string `yaml:"fields"`
	TimestampIndex  string `yaml:"timestamp_index"` //第几个字段是时间
	TimestampFormat string `yaml:"timestamp_format"`
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
	log.Printf("Starting kafka consumer")

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
		fields := make(map[string]interface{})
		data := strings.Split(line, kafkaServer.config.SeparatorChar)
		//log.Print(data)
		for _,column := range strings.Split(kafkaServer.config.Fields, ",") {
			column_name := strings.Split(column, ":")[0]
			column_index, err := strconv.Atoi(strings.Split(column, ":")[1])
			if err != nil {
				log.Printf(err.Error())
				os.Exit(-1)
			}
			if len(data)>column_index {
				//log.Printf("%s %d %d",column_name,column_index,len(data))
				fields[column_name] = data[column_index]
			}

		}

		//处理时间字段
		t_index, err := strconv.Atoi(kafkaServer.config.TimestampIndex)
		if err != nil {
			log.Printf("timestamp_index must be a number")
			os.Exit(-1)
		}
		tt, _ := time.Parse(kafkaServer.config.TimestampFormat, data[t_index])
		fields["@timestamp"] = tt.Format(time.RFC3339Nano)
		ev.Fields = &fields
		json_text, err := json.Marshal(fields)
		text := string(json_text)
		ev.Text = &text
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

