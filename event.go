package avrostry

// DomainEvent Struct for Implement the Domain Event.
type DomainEvent interface {
	AvroSchema() string
	Version() int
	Subject() string
	// TODO Create the way to generalize this.
	//FromPayload(m map[string]interface{}) error
	ToPayload() map[string]interface{}
	AggregateID() interface{}
}
