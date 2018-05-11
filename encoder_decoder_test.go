package avrostry_test

import (
	"encoding/json"
	"testing"

	"github.com/josgilmo/avrostry"
)

const (
	schemaRepositoryUrl = "http://localhost:8081"
	rawMetricsSchema    = `{"namespace": "ly.stealth.kafka.metrics","type": "record","name": "Timings","fields": [{"name": "id", "type": "long"},{"name": "timings",  "type": {"type":"array", "items": "long"} }]}`
)

func assert(t *testing.T, value interface{}, expected interface{}) {
	if value != expected {
		t.Errorf("Value %v, expected %v", value, expected)
	}
}

func assertNot(t *testing.T, value interface{}, expected interface{}) {
	if value == expected {
		t.Errorf("Value %v, expected %v", value, expected)
	}
}

type WordWasRead struct {
	Word string
}

func (word WordWasRead) AvroSchema() string {
	return `{
		"type": "record",
		"name": "words",
		"doc:": "Just words",
		"namespace": "com.avro.kafka.golang",
		"fields": [
		{
			"type": "string",
			"name": "Word"
		}
		]
	}
	`
}

func (word *WordWasRead) FromPayload(m map[string]interface{}) error {

	data, _ := json.Marshal(m)
	err := json.Unmarshal(data, word)

	return err
}

func (word WordWasRead) ToPayload() map[string]interface{} {
	datumIn := map[string]interface{}{
		"Word": word.Word,
	}

	return datumIn
}

func (word WordWasRead) Version() int {
	return 1
}
func (word WordWasRead) Subject() string {
	return "ddd:words:read"
}

func TestAvroKafkaEncoderDecoder(t *testing.T) {
	word := WordWasRead{Word: "Palabro"}

	client := avrostry.NewCachedSchemaRegistryClient("http://localhost:8081")
	idCache := make(map[int32]string)
	var schemaIdMap map[string]int32
	schemaIdMap = make(map[string]int32)
	rawSchema := word.AvroSchema()

	schemaIdMap[rawSchema] = 1
	client.SchemaCache[word.Subject()] = schemaIdMap
	client.IdCache = idCache
	idCache[1] = rawSchema

	encoder := avrostry.NewKafkaAvroEncoder(schemaRepositoryUrl)
	encoder.SchemaRegistry = client

	bytes, err := encoder.Encode(word)
	assert(t, err, nil)
	assertNot(t, bytes, nil)

	decoder := avrostry.NewKafkaAvroDecoder(schemaRepositoryUrl)
	decoder.SchemaRegistry = client
	obj, err := decoder.Decode(bytes)
	if err != nil {
		t.Errorf("Returned: %v", err)
	}
	wordDecoded := &WordWasRead{}

	err = wordDecoded.FromPayload(obj.(map[string]interface{}))
	if err != nil {
		t.Errorf("Error %v", err)
	}
	if wordDecoded.Word != word.Word {
		t.Errorf("Wrong word returned: %v", obj)
	}
}
