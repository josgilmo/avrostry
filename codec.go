package avrostry

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

var (
	magicBytes = []byte{0}
)

// KafkaAvroCodec
type KafkaAvroCodec struct {
	schemaRegistry SchemaRegistryClient
	cacheCodec     *CacheCodec
}

func NewKafkaAvroCodec(s SchemaRegistryClient, cache *CacheCodec) *KafkaAvroCodec {
	return &KafkaAvroCodec{s, cache}
}

// Encode Given a DomainEvent interface, encode the message,
// loading the struct to/from Kafka Schema Registry.
func (kac *KafkaAvroCodec) Encode(event DomainEvent) ([]byte, error) {
	id, err := kac.schemaRegistry.Register(event.Subject(), event.AvroSchema())
	if err != nil {
		return nil, err
	}

	//
	// MagicByte(1) + SchemaID(4) + SubjectLen(1) + Subject + EventData
	//
	buffer := bytes.NewBuffer(magicBytes)

	// Schema ID
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(id))
	_, err = buffer.Write(buf)
	if err != nil {
		return nil, err
	}

	// Event subject
	buf = make([]byte, 1)
	subjectLen := len(event.Subject())
	if subjectLen > 255 {
		return nil, fmt.Errorf("subject: %s, longer than 255", event.Subject())
	}
	buf[0] = byte(subjectLen)
	_, err = buffer.Write(buf)
	if err != nil {
		return nil, err
	}
	_, err = buffer.WriteString(event.Subject())
	if err != nil {
		return nil, err
	}

	codec, err := kac.cacheCodec.Get(event.AvroSchema())
	if err != nil {
		return nil, err
	}

	binary, err := codec.BinaryFromNative(nil, event.ToStringMap())
	if err != nil {
		return nil, err
	}
	_, err = buffer.Write(binary)

	return buffer.Bytes(), err
}

func (kac *KafkaAvroCodec) Decode(buf []byte) (string, interface{}, error) {
	//
	// MagicByte(1) + SchemaID(4) + SubjectLen(1) + Subject + EventData
	//
	n := len(buf)
	if n < 6 {
		return "", nil, fmt.Errorf("message len: %d, shorter than 6 bytes", n)
	}

	// Magic byte
	if buf[0] != 0 {
		return "", nil, errors.New("unknown magic byte")
	}

	// Schema ID
	id := int32(binary.BigEndian.Uint32(buf[1:5]))
	schema, err := kac.schemaRegistry.GetByID(id)
	if err != nil {
		return "", nil, err
	}

	// Event subject
	subjectLen := int(buf[5])
	if subjectLen > n-6 {
		return "", nil, fmt.Errorf("subjectLen len: %d, greater than remaining buffer: %d", subjectLen, n-6)
	}

	subject := string(buf[6 : 6+subjectLen])
	codec, err := kac.cacheCodec.Get(schema)
	if err != nil {
		return "", nil, err
	}

	native, _, err := codec.NativeFromBinary(buf[6+subjectLen:])
	if err != nil {
		return "", nil, err
	}

	return subject, native, nil
}
