package amqp

import "github.com/datazip-inc/olake/utils"

type Config struct {
	Normalization bool   `json:"normalization,omitempty"`
	Url           string `json:"url,omitempty"`
	Topic         string `json:"topic,omitempty"`
	RoutingKey    string `json:"routingKey,omitempty"`
	Mandatory     bool   `json:"mandatory,omitempty"`
	Immediate     bool   `json:"immediate,omitempty"`
	AutoCreate    bool   `json:"autoCreate,omitempty"`
	ExchangeName  string `json:"exchangeName,omitempty"`
	ExchangeType  string `json:"exchangeType,omitempty"`
	QueueName     string `json:"queueName,omitempty"`
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
