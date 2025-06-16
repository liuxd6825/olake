package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/rabbitmq/amqp091-go"
	"time"
)

type AmqpWriter struct {
	config  *Config
	conn    *amqp091.Connection
	channel *amqp091.Channel
	options *destination.Options
	stream  types.StreamInterface
}

func NewAmqpWriter() destination.Writer {
	return &AmqpWriter{
		config: &Config{},
	}
}

func (m *AmqpWriter) GetConfigRef() destination.Config {
	return m.config
}

func (m *AmqpWriter) Spec() any {
	return Config{}
}

func (m *AmqpWriter) Check(ctx context.Context) error {
	return m.initMQ()
}

func (m *AmqpWriter) Type() string {
	return string(types.AMQP)
}

func (m *AmqpWriter) initMQ() (err error) {
	conn, err := amqp091.Dial(m.config.Url)
	if err != nil {
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	m.conn = conn
	m.channel = ch
	return m.autoCreate()
}

func (m *AmqpWriter) autoCreate() (err error) {
	if !m.config.AutoCreate {
		return nil
	}
	ch := m.channel
	// 3. 声明一个 Exchange
	err = ch.ExchangeDeclare(
		m.config.ExchangeName, // 交换机名称
		m.config.ExchangeType, // 类型，direct 是最常见的一种类型
		true,                  // 是否持久化
		false,                 // 是否自动删除
		false,                 // 是否内建
		false,                 // 是否等待确认
		nil,                   // 额外参数
	)
	if err != nil {
		return errors.New(fmt.Sprintf("无法声明 Exchange: %s", err))
	}

	// 4. 声明一个 Queue
	_, err = ch.QueueDeclare(
		m.config.QueueName, // 队列名称
		true,               // 是否持久化
		false,              // 是否自动删除
		false,              // 是否排他
		false,              // 是否等待确认
		nil,                // 额外参数
	)
	if err != nil {
		return errors.New(fmt.Sprintf("无法声明 Queue: %s", err))
	}

	// 5. 将 Queue 绑定到 Exchange
	err = ch.QueueBind(
		m.config.QueueName,    // 队列名称
		m.config.RoutingKey,   // 路由键
		m.config.ExchangeName, // 交换机名称
		false,                 // 是否等待确认
		nil,                   // 额外参数
	)
	if err != nil {
		return errors.New(fmt.Sprintf("无法绑定 Queue 到 Exchange: %s", err))
	}
	return nil
}

func (m *AmqpWriter) Setup(stream types.StreamInterface, opts *destination.Options) error {
	m.options = opts
	m.stream = stream
	return nil
}

func (m *AmqpWriter) Write(ctx context.Context, record types.RawRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	logger.Infof("amqp db:%s; table:%s; operationType:%s, after:%v; before:%v", record.DB, record.Table, record.OperationType, record.After, record.Before)
	msg := amqp091.Publishing{
		ContentType: "text/plain",
		Body:        data,
	}
	return m.channel.Publish(m.config.ExchangeName, m.config.RoutingKey, m.config.Mandatory, m.config.Immediate, msg)
}

func (m *AmqpWriter) Normalization() bool {
	return m.config.Normalization
}

func (m *AmqpWriter) Flattener() destination.FlattenFunction {
	flattener := typeutils.NewFlattener()
	return flattener.Flatten
}

func (m *AmqpWriter) EvolveSchema(b bool, b2 bool, m2 map[string]*types.Property, record types.Record, time time.Time) error {
	return nil
}

func (m *AmqpWriter) Close(ctx context.Context) error {
	//m.channel.Close()
	// return m.conn.Close()
	return nil
}

func init() {
	writer := NewAmqpWriter()
	destination.RegisteredWriters[types.AMQP] = func() destination.Writer {
		return writer
	}
}
