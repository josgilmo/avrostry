package avrostry

// DomainEvent is the interface every that event
// we want to publish in Kafka must implement
type DomainEvent interface {
	// This event schema according to Avro spec
	AvroSchema() string

	// The type name of the event, is the selector
	// to instantiate a concrete event after decoding
	Subject() string

	// Convert to a map for encoding
	ToStringMap() map[string]interface{}

	// ID of the event, it will be the partition key
	ID() string
}

// Message sending header
type MessageHeader struct {
	Key   string
	Value string
}
