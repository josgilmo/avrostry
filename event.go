package avrostry

// DomainEvent Struct for Implement the Domain Event.
type DomainEvent interface {
	AvroSchema() string
	Version() int
	Subject() string
	// TODO Create the way to generalize this.
	//FromPayload(m map[string]interface{}) error
	ToPayload() interface{}
	// TODO Change to string
	AggregateID() interface{}
}

//TODO Split of DomainEvent interface to one for producer and other for consumer (.

type ConsumerEvent interface {
	Subject() string
	// TODO Create the way to generalize this.
	//FromPayload(m map[string]interface{}) error
	ToPayload() map[string]interface{}
	// TODO Change to string
	ID() interface{}
}
