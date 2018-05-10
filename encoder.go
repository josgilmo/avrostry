package avrostry

import (
	"encoding/binary"
	"errors"

	avro "github.com/stealthly/go-avro"
)

type KafkaAvroDecoder struct {
	schemaRegistry SchemaRegistryClient
}

func NewKafkaAvroDecoder(url string) *KafkaAvroDecoder {
	return &KafkaAvroDecoder{
		schemaRegistry: NewCachedSchemaRegistryClient(url),
	}
}

func (this *KafkaAvroDecoder) Decode(bytes []byte) (interface{}, error) {
	if bytes == nil {
		return nil, nil
	} else {
		if bytes[0] != 0 {
			return nil, errors.New("Unknown magic byte!")
		}
		id := int32(binary.BigEndian.Uint32(bytes[1:]))
		schema, err := this.schemaRegistry.GetByID(id)
		if err != nil {
			return nil, err
		}

		if schema.Type() == avro.Bytes {
			return bytes[5:], nil
		} else {
			reader := avro.NewGenericDatumReader()
			reader.SetSchema(schema)
			value := avro.NewGenericRecord(schema)
			err := reader.Read(value, avro.NewBinaryDecoder(bytes[5:]))

			return value, err
		}
	}
}

func (this *KafkaAvroDecoder) DecodeSpecific(bytes []byte, value interface{}) error {
	if bytes == nil {
		return nil
	} else {
		if bytes[0] != 0 {
			return errors.New("Unknown magic byte!")
		}
		id := int32(binary.BigEndian.Uint32(bytes[1:]))
		schema, err := this.schemaRegistry.GetByID(id)
		if err != nil {
			return err
		}

		reader := avro.NewSpecificDatumReader()
		reader.SetSchema(schema)
		return reader.Read(value, avro.NewBinaryDecoder(bytes[5:]))
	}
}
