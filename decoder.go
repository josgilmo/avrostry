package avrostry

import (
	"encoding/binary"
	"errors"

	"github.com/linkedin/goavro"
)

// KafkaAvroDecoder Struct that implement the Decode function
type KafkaAvroDecoder struct {
	SchemaRegistry SchemaRegistryClient
}

// NewKafkaAvroDecoder Create a KafkaAvroDecoder struct
func NewKafkaAvroDecoder(url string) *KafkaAvroDecoder {
	return &KafkaAvroDecoder{
		SchemaRegistry: NewSchemaRegistryManager(url),
	}
}

//Decode Return the decode message related to it´s Schema
func (kafkaAvroDecoder *KafkaAvroDecoder) Decode(bytes []byte) (interface{}, error) {
	if bytes == nil {
		return nil, nil
	}
	if bytes[0] != 0 {
		return nil, errors.New("Unknown magic byte")
	}
	id := int32(binary.BigEndian.Uint32(bytes[1:]))
	schema, err := kafkaAvroDecoder.SchemaRegistry.GetByID(id)
	if err != nil {
		return nil, err
	}

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, err
	}

	native, _, err := codec.NativeFromBinary(bytes[5:])

	return native, err
}
