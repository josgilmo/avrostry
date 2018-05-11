package avrostry

import "github.com/linkedin/goavro"

// TODO: move as member and lock with sync.RLock
var RegisteredCodecEvents map[string]*goavro.Codec

type DomainEvent interface {
	AvroSchema() string
	Version() int
	Subject() string
	// TODO Create the way to generalize this.
	//FromPayload(m map[string]interface{}) error
	ToPayload() map[string]interface{}
}
