package types

import (
	"errors"
	"fmt"
	"github.com/datazip-inc/olake/constants"
	"github.com/goccy/go-json"
	"sort"
	"time"
)

type Record map[string]any

type RawRecord struct {
	Schema         StreamInterface
	Before         map[string]any `parquet:"before,json" json:"before,omitempty"`
	After          map[string]any `parquet:"after,json" json:"after,omitempty"`
	OlakeID        string         `parquet:"_olake_id" json:"olakeId,omitempty"`
	OlakeTimestamp time.Time      `parquet:"_olake_timestamp" json:"olakeTimestamp"`
	OperationType  string         `parquet:"_op_type" json:"opType" ` // "r" for read/backfill, "c" for create, "u" for update, "d" for delete
	CdcTimestamp   time.Time      `parquet:"_cdc_timestamp" json:"cdcTimestamp"`
	DB             string         `parquet:"db" json:"db"`        //数据库
	Table          string         `parquet:"table"  json:"table"` //数据表
}

func CreateRawRecord(schema StreamInterface, olakeID string, before, after map[string]any, operationType string, cdcTimestamp time.Time, dbName, table string) RawRecord {
	return RawRecord{
		Schema:        schema,
		OlakeID:       olakeID,
		Before:        before,
		After:         after,
		OperationType: operationType,
		CdcTimestamp:  cdcTimestamp,
		DB:            dbName,
		Table:         table,
	}
}

func (r *RawRecord) ToDebeziumFormat(db string, stream string, normalization bool) (string, error) {
	// First create the schema and track field types
	schema := r.createDebeziumSchema(db, stream, normalization)

	// Create the payload with the actual data
	payload := make(map[string]interface{})

	// Add olake_id to payload
	payload[constants.OlakeID] = r.OlakeID

	// Handle data based on normalization flag
	if normalization {
		for key, value := range r.After {
			payload[key] = value
		}
	} else {
		dataBytes, err := json.Marshal(r.After)
		if err != nil {
			return "", err
		}
		payload["after"] = string(dataBytes)
	}

	// Add the metadata fields
	payload[constants.OpType] = r.OperationType // "r" for read/backfill, "c" for create, "u" for update
	payload[constants.DBName] = db
	payload[constants.CdcTimestamp] = r.CdcTimestamp
	payload[constants.OlakeTimestamp] = r.OlakeTimestamp

	// Create Debezium format
	debeziumRecord := map[string]interface{}{
		"destination_table": stream,
		"key": map[string]interface{}{
			"schema": map[string]interface{}{
				"type": "struct",
				"fields": []map[string]interface{}{
					{
						"type":     "string",
						"optional": true,
						"field":    constants.OlakeID,
					},
				},
				"optional": false,
			},
			"payload": map[string]interface{}{
				constants.OlakeID: r.OlakeID,
			},
		},
		"value": map[string]interface{}{
			"schema":  schema,
			"payload": payload,
		},
	}

	jsonBytes, err := json.Marshal(debeziumRecord)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil
}

func (r *RawRecord) createDebeziumSchema(db string, stream string, normalization bool) map[string]interface{} {
	fields := make([]map[string]interface{}, 0)

	// Add olake_id field first
	fields = append(fields, map[string]interface{}{
		"type":     "string",
		"optional": true,
		"field":    constants.OlakeID,
	})

	if normalization {
		// Collect data fields for sorting
		dataFields := make([]map[string]interface{}, 0, len(r.After))

		// Add individual data fields
		for key, value := range r.After {
			field := map[string]interface{}{
				"optional": true,
				"field":    key,
			}

			switch value.(type) {
			case bool:
				field["type"] = "boolean"
			case int, int8, int16, int32:
				field["type"] = "int32"
			case int64:
				field["type"] = "int64"
			case float32:
				field["type"] = "float32"
			case float64:
				field["type"] = "float64"
			case time.Time:
				field["type"] = "timestamptz" // use with timezone as we use default utc
			default:
				field["type"] = "string"
			}

			dataFields = append(dataFields, field)
		}

		// Sorting basis on field names is needed because
		// Iceberg writer detects different schemas for
		// schema evolution based on order columns passed
		sort.Slice(dataFields, func(i, j int) bool {
			return dataFields[i]["field"].(string) < dataFields[j]["field"].(string)
		})

		fields = append(fields, dataFields...)
	} else {
		// For non-normalized mode, add a single data field as string
		fields = append(fields, map[string]interface{}{
			"type":     "string",
			"optional": true,
			"field":    "data",
		})
	}

	// Add metadata fields
	fields = append(fields, []map[string]interface{}{
		{
			"type":     "string",
			"optional": true,
			"field":    constants.OpType,
		},
		{
			"type":     "string",
			"optional": true,
			"field":    constants.DBName,
		},
		{
			"type":     "timestamptz",
			"optional": true,
			"field":    constants.CdcTimestamp,
		},
		{
			"type":     "timestamptz",
			"optional": true,
			"field":    constants.OlakeTimestamp,
		},
	}...)

	return map[string]interface{}{
		"type":     "struct",
		"fields":   fields,
		"optional": false,
		"name":     fmt.Sprintf("%s.%s", db, stream),
	}
}

type DomainEvent struct {
	EventType string
	//Data      any
	//Meta      any
}

func (r *RawRecord) IsDomainEvent() bool {
	if r.Schema != nil && r.Schema.GetStream() != nil {
		cfg := r.Schema.GetStream().DomainEvent
		if cfg != nil && cfg.EventType != nil && len(cfg.EventType) > 0 && r.OperationType != "d" {
			return true
		}
	}
	return false
}

func (r *RawRecord) IsSendEvent() bool {
	return r.OperationType != "d"
}

func (r *RawRecord) GetDomainEvent() (domainEvent *DomainEvent, err error) {
	if r.IsDomainEvent() {
		cfg := r.Schema.GetStream().DomainEvent
		eventType, err := getArrayString(r.After, cfg.EventType)
		if err != nil {
			return nil, err
		}
		domainEvent = &DomainEvent{
			EventType: eventType,
		}
		return domainEvent, nil

	}
	return nil, nil
}

func getArrayString(m map[string]any, keys []string) (string, error) {
	var res string
	for _, key := range keys {
		if val, ok := m[key]; ok {
			if str, ok := val.(string); ok {
				if res == "" {
					res = str
				} else {
					res = res + "." + str
				}
			} else {
				return "", errors.New(fmt.Sprintf("field %s is not string", key))
			}
		} else {
			return "", errors.New(fmt.Sprintf("field %s not found", key))
		}
	}
	return res, nil
}

func getAny(m map[string]any, key string) (any, error) {
	if val, ok := m[key]; ok {
		return val, nil
	}
	return "", errors.New("key not found")
}
