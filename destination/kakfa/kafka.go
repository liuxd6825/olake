package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/typeutils"
	"time"
)

type KafkaWriter struct {
	config   *Config
	producer sarama.SyncProducer
	options  *destination.Options
	stream   types.StreamInterface
}

func NewKafkaWriter() destination.Writer {
	return &KafkaWriter{
		config: &Config{},
	}
}

func (m *KafkaWriter) GetConfigRef() destination.Config {
	return m.config
}

func (m *KafkaWriter) Spec() any {
	return Config{}
}

func (m *KafkaWriter) Check(ctx context.Context) error {
	return m.initMQ()
}

func (m *KafkaWriter) Type() string {
	return string(types.KAFKA)
}

func (m *KafkaWriter) initMQ() (err error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true // 确保生产者返回成功
	cfg.Producer.Retry.Max = 3
	cfg.Producer.RequiredAcks = sarama.WaitForAll // 等待所有副本确认

	producer, err := sarama.NewSyncProducer(m.config.Address, cfg)
	if err != nil {
		return err
	}

	m.producer = producer
	return m.autoCreate(cfg)
}

func (m *KafkaWriter) autoCreate(config *sarama.Config) (err error) {
	if !m.config.AutoCreate {
		return nil
	}

	// 创建 AdminClient
	admin, err := sarama.NewClusterAdmin(m.config.Address, config)
	if err != nil {
		return errors.New(fmt.Sprintf("无法连接到 Kafka Admin: %v", err))
	}
	defer admin.Close()

	// 定义 Topic 配置
	topic := m.config.Topic
	details := &sarama.TopicDetail{
		NumPartitions:     1, // 分区数
		ReplicationFactor: 1, // 副本数
	}

	// 创建 Topic
	err = admin.CreateTopic(topic, details, false)
	if err != nil && !errors.Is(err, sarama.ErrTopicAlreadyExists) {
		return errors.New(fmt.Sprintf("无法创建 Topic: %v", err))
	}
	return nil
}

func (m *KafkaWriter) Setup(stream types.StreamInterface, opts *destination.Options) error {
	m.options = opts
	m.stream = stream
	return nil
}

func (m *KafkaWriter) Write(ctx context.Context, record types.RawRecord) (err error) {
	var data []byte
	var topic string
	if dEvent, err := record.GetDomainEvent(); err != nil {
		return err
	} else if dEvent != nil {
		topic = dEvent.EventType
		data, err = json.Marshal(record.After)
		if err != nil {
			return err
		}
	} else {
		topic = m.config.Topic
		if topic == "" {
			topic = fmt.Sprintf("%s.%s", record.DB, record.Table)
		}
		data, err = json.Marshal(record)
		if err != nil {
			return err
		}
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(data),
	}
	_, _, err = m.producer.SendMessage(msg)
	return err
}

func (m *KafkaWriter) Normalization() bool {
	return m.config.Normalization
}

func (m *KafkaWriter) Flattener() destination.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func (m *KafkaWriter) EvolveSchema(b bool, b2 bool, m2 map[string]*types.Property, record types.Record, time time.Time) error {
	return nil
}

func (m *KafkaWriter) Close(ctx context.Context) error {
	return m.producer.Close()
}

func init() {
	writer := NewKafkaWriter()
	destination.RegisteredWriters[types.KAFKA] = func() destination.Writer {
		return writer
	}
}
