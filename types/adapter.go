package types

type AdapterType string

const (
	Parquet AdapterType = "PARQUET"
	Iceberg AdapterType = "ICEBERG"
	AMQP    AdapterType = "AMQP"
	KAFKA   AdapterType = "KAFKA"
)

// TODO: Add validations
type WriterConfig struct {
	Type         AdapterType `json:"type"`
	WriterConfig any         `json:"writer"`
}
