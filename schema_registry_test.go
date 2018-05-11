/* Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License") you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */

package avrostry

import (
	"testing"
)

func assert(t *testing.T, value error, expected interface{}) {
	if value != expected {
		t.Errorf("Value was %v, expected %v", value, expected)
	}
}

func assertNot(t *testing.T, value interface{}, expected interface{}) {

}

func TestSchemaRegistryCached(t *testing.T) {
	client := NewCachedSchemaRegistryClient("http://localhost:8081")
	idCache := make(map[int32]string)
	var schemaIdMap map[string]int32
	schemaIdMap = make(map[string]int32)
	rawSchema := "{\"namespace\": \"ly.stealth.kafka.metrics\",\"type\": \"record\",\"name\": \"Timings\",\"fields\": [{\"name\": \"id\", \"type\": \"long\"},{\"name\": \"timings\",  \"type\": {\"type\":\"array\", \"items\": \"long\"} }]}"
	schemaIdMap[rawSchema] = 1
	client.SchemaCache["test1"] = schemaIdMap
	client.IdCache = idCache
	idCache[1] = rawSchema

	// schema, err := avro.ParseSchema(rawSchema)
	// assert(t, err, nil)
	id, err := client.Register("test1", rawSchema)

	assert(t, err, nil)
	assertNot(t, id, 0)

	schema, err := client.GetByID(id)
	assert(t, err, nil)
	assertNot(t, schema, nil)

	// metadata, err := client.GetLatestSchemaMetadata("test1")
	// assert(t, err, nil)
	// assertNot(t, metadata, nil)

	//version, err := client.GetVersion("test1", schema)
	// assert(t, err, nil)
	// assertNot(t, version, 0)
}
