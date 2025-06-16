package kafka

import "github.com/datazip-inc/olake/utils"

type Config struct {
	Normalization bool     `json:"normalization,omitempty"`
	Address       []string `json:"address,omitempty"`
	Topic         string   `json:"topic,omitempty"`
	AutoCreate    bool     `json:"autoCreate,omitempty"`
}

func (c *Config) Validate() error {
	return utils.Validate(c)
}
