package avrostry

import (
	"testing"
)

func TestSchemaRegistryCached(t *testing.T) {
	/*
		client := NewSchemaRegistryManager("http://localhost:8081")
		idCache := make(map[int32]string)
		var schemaIdMap map[string]int32
		schemaIdMap = make(map[string]int32)
		rawSchema := "{\"namespace\": \"ly.stealth.kafka.metrics\",\"type\": \"record\",\"name\": \"Timings\",\"fields\": [{\"name\": \"id\", \"type\": \"long\"},{\"name\": \"timings\",  \"type\": {\"type\":\"array\", \"items\": \"long\"} }]}"
		schemaIdMap[rawSchema] = 1
		client.SchemaCache["test1"] = schemaIdMap
		client.IdCache = idCache
		idCache[1] = rawSchema

		id, err := client.Register("test1", rawSchema)

		assert(t, err, nil)
		assertNot(t, id, 0)

		schema, err := client.GetByID(id)
		assert(t, err, nil)
		assertNot(t, schema, nil)
	*/
}
