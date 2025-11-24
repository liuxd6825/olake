package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/datazip-inc/olake/destination"
	"github.com/datazip-inc/olake/types"
	"github.com/datazip-inc/olake/utils/logger"
	"github.com/datazip-inc/olake/utils/typeutils"
	"github.com/rabbitmq/amqp091-go"
)

type AmqpWriter struct {
	config    *Config
	conn      *amqp091.Connection
	channel   *amqp091.Channel
	options   *destination.Options
	stream    types.StreamInterface
	mutex     sync.Mutex
	closeChan chan struct{}
}

func NewAmqpWriter() destination.Writer {
	return &AmqpWriter{
		config:    &Config{},
		closeChan: make(chan struct{}),
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
	err = m.connect()
	if err != nil {
		return err
	}
	if err := m.autoCreate(); err != nil {
		return err
	}
	return nil
}

type ExchangeType string

/*
Exchange
类型使用哪种    key	           路由规则    	       消息分发方式  	         主要场景
Direct	      routingKey	   精确匹配	           点对点 / 精准发放	     任务分发、明确消费者处理
Fanout	      不使用	           广播至所有绑定队列	   广播广播广播	         通知、日志广播、配置下发
Topic	      routingKey	   支持 *, # 模糊匹配	   有选择的多播	         系统日志、按模块分类、地理分组
Headers	      message          headers	           多属性匹配（any/all）	 属性路由	多维属性过滤、复杂路由需求
*/
const (
	Direct  ExchangeType = "direct"
	Topic   ExchangeType = "topic"
	Fanout  ExchangeType = "fanout"
	Headers ExchangeType = "headers"
)

func (m *AmqpWriter) autoCreate() (err error) {
	if !m.config.AutoCreate {
		return nil
	}
	ch := m.channel
	if ch.IsClosed() {
		return errors.New("无法声明 Queue: channel is closed")
	}

	// 3. 声明一个 Exchange
	err = ch.ExchangeDeclare(
		m.config.ExchangeName, // 交换机名称
		m.config.ExchangeType, //  m.config.ExchangeType, // 类型，direct 是最常见的一种类型 , topic
		m.config.Durable,      // 是否持久化
		m.config.AutoDelete,   // 是否自动删除
		m.config.Exclusive,    // 是否排他
		m.config.NoWait,       // 是否等待确认
		amqp091.Table{
			//"x-message-ttl": int32(6000),
		}, // 额外参数
	)
	if err != nil {
		return errors.New(fmt.Sprintf("无法声明 Exchange: %s", err))
	}
	hasQueue := true
	_, err = ch.QueueDeclarePassive(m.config.QueueName, true, false, false, false, nil)
	if err != nil {
		if amqpErr, ok := err.(*amqp091.Error); ok {
			if amqpErr.Code == 404 { // 404 是 "NOT_FOUND" 错误码
				hasQueue = false
			} else {
				return err
			}
		} else {
			return err
		}
	}
	if hasQueue {
		return nil
	}

	if ch.IsClosed() {
		if err = m.reconnect(); err != nil {
			return errors.New(fmt.Sprintf("无法声明 Queue: channel is closed %s", err.Error()))
		}
	}

	ch = m.channel
	// 4. 声明一个
	_, err = ch.QueueDeclare(
		m.config.QueueName,  // 队列名称
		m.config.Durable,    // 是否持久化
		m.config.AutoDelete, // 是否自动删除
		m.config.Exclusive,  // 是否排他
		m.config.NoWait,     // 是否等待确认
		amqp091.Table{
			//"x-message-ttl": int32(6000),
		}, // 额外参数
	)
	if err != nil {
		return errors.New(fmt.Sprintf("无法声明 Queue: %s", err))
	}

	// 5. 将 Queue 绑定到 Exchange
	err = ch.QueueBind(
		m.config.QueueName,    // 队列名称
		m.config.RoutingKey,   // 路由键
		m.config.ExchangeName, // 交换机名称
		m.config.NoWait,       // 是否等待确认
		amqp091.Table{
			//"x-message-ttl": int32(6000),
		}, // 额外参数
	)

	return nil
}

func (m *AmqpWriter) connect() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 关闭现有连接（如果存在）
	if m.channel != nil {
		_ = m.channel.Close()
	}
	if m.conn != nil {
		_ = m.conn.Close()
	}

	// 建立新连接
	conn, err := amqp091.Dial(m.config.Url)
	if err != nil {
		return fmt.Errorf("dial failed: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("channel create failed: %v", err)
	}

	m.conn = conn
	m.channel = ch
	return nil
}

func (m *AmqpWriter) reconnect() error {
	// 最大重试次数和初始延迟
	const maxRetries = 5
	initialDelay := time.Second

	var lastErr error

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			// 指数退避
			delay := initialDelay * time.Duration(math.Pow(2, float64(i-1)))
			if delay > 30*time.Second {
				delay = 30 * time.Second
			}
			time.Sleep(delay)
		}

		if err := m.connect(); err == nil {
			return nil // 连接成功
		} else {
			lastErr = err
			logger.Infof("Reconnect attempt %d failed: %v", i+1, err)
		}
	}

	return fmt.Errorf("after %d reconnect attempts, last error: %v", maxRetries, lastErr)
}

func (m *AmqpWriter) Setup(stream types.StreamInterface, opts *destination.Options) error {
	m.options = opts
	m.stream = stream
	return nil
}

func (m *AmqpWriter) Write(ctx context.Context, record types.RawRecord) (err error) {
	var data []byte
	var routingKey string

	if record.IsDomainEvent() && !record.IsSendEvent() {
		return nil
	}

	if dEvent, err := record.GetDomainEvent(); err != nil {
		return err
	} else if dEvent != nil {
		data, err = json.Marshal(record.After)
		if err != nil {
			return err
		}
		routingKey = dEvent.EventType
	} else {
		data, err = json.Marshal(record)
		if err != nil {
			return err
		}
		routingKey = m.config.RoutingKey
		if m.stream.GetStream().RouteKey != "" {
			routingKey = m.stream.GetStream().RouteKey
		}
		if routingKey == "" {
			routingKey = fmt.Sprintf("%s.%s", record.DB, record.Table)
		}
	}
	msg := amqp091.Publishing{
		ContentType: "text/plain",
		Body:        data,
	}

	err = m.channel.Publish(m.config.ExchangeName, routingKey, m.config.Mandatory, m.config.Immediate, msg)
	if err == nil {
		logger.Infof("amqp exchange:%s; routingKey:%s; db:%s; table:%s; operationType:%s", m.config.ExchangeName, routingKey, record.DB, record.Table, record.OperationType)
	}
	return err
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
