package types

import (
	"github.com/parquet-go/parquet-go"
)

type DataType string

const (
	Null           DataType = "null"
	Int32          DataType = "integer_small"
	Int64          DataType = "integer"
	Float32        DataType = "number_small"
	Float64        DataType = "number"
	String         DataType = "string"
	Bool           DataType = "boolean"
	Object         DataType = "object"
	Array          DataType = "array"
	Unknown        DataType = "unknown"
	Timestamp      DataType = "timestamp"
	TimestampMilli DataType = "timestamp_milli" // storing datetime up to 3 precisions
	TimestampMicro DataType = "timestamp_micro" // storing datetime up to 6 precisions
	TimestampNano  DataType = "timestamp_nano"  // storing datetime up to 9 precisions
)

// Tree Representation of TypeWeights
//
//                                5 (String)
//                               /       	 \
//                3 (Float64)  /              \ 9 (TimestampNano)
//                           /  \             /
//             2 (Int64)   /     \ 4(Float32)/ 8 (TimestampMicro)
//                        /                 /
//            1 (Int32) /                  / 7 (TimestampMilli)
//                     /                  /
//        0 (Bool)   /                   / 6 (Timestamp)
//

var TypeWeights = map[DataType]int{
	Bool:           0,
	Int32:          1,
	Int64:          2,
	Float64:        3,
	Float32:        4,
	String:         5,
	TimestampNano:  9,
	TimestampMicro: 8,
	TimestampMilli: 7,
	Timestamp:      6,
}

func (d DataType) ToNewParquet() parquet.Node {
	var n parquet.Node

	switch d {
	case Int32:
		n = parquet.Leaf(parquet.Int32Type)
	case Float32:
		n = parquet.Leaf(parquet.FloatType)
	case Int64:
		n = parquet.Leaf(parquet.Int64Type)
	case Float64:
		n = parquet.Leaf(parquet.DoubleType)
	case String:
		n = parquet.String()
	case Bool:
		n = parquet.Leaf(parquet.BooleanType)
	case Timestamp, TimestampMilli, TimestampMicro, TimestampNano:
		n = parquet.Timestamp(parquet.Microsecond)
	case Object, Array:
		// Ensure proper handling of nested structures
		n = parquet.String()
	default:
		n = parquet.Leaf(parquet.ByteArrayType)
	}

	n = parquet.Optional(n) // Ensure the field is nullable
	return n
}
