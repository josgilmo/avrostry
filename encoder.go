package avrostry

import (
	"bytes"
	"encoding/binary"

	"github.com/linkedin/goavro"
)

var magicBytes = []byte{0}

type KafkaAvroEncoder struct {
	SchemaRegistry SchemaRegistryClient
}

func NewKafkaAvroEncoder(url string) *KafkaAvroEncoder {

	return &KafkaAvroEncoder{
		SchemaRegistry: NewCachedSchemaRegistryClient(url),
	}
}

func (this *KafkaAvroEncoder) Encode(event DomainEvent) ([]byte, error) {

	id, err := this.SchemaRegistry.Register(event.Subject(), event.AvroSchema())
	if err != nil {
		return nil, err
	}

	buffer := &bytes.Buffer{}
	buffer.Write(magicBytes)
	idSlice := make([]byte, 4)
	binary.BigEndian.PutUint32(idSlice, uint32(id))
	buffer.Write(idSlice)

	// TODO: Cache codecs
	codec, err := goavro.NewCodec(event.AvroSchema())
	if err != nil {
		return nil, err
	}
	binary, err := codec.BinaryFromNative(nil, event.ToPayload())
	buffer.Write(binary)

	return buffer.Bytes(), err
}
