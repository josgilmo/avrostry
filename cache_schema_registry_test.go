package avrostry

import "testing"

func TestGetByID(t *testing.T) {
	cache := NewCacheSchemaRegistry()
	cache.SetBySubjectSquema("subject", "schema", 1)

	schema, found := cache.GetByID(1)
	if schema != "schema" {
		t.Fail()
	}
	if !found {
		t.Fail()
	}
}

func TestGetIDBySubjectAndSquemaSimpleCase(t *testing.T) {
	cache := NewCacheSchemaRegistry()
	cache.SetBySubjectSquema("subject", "schema", 1)

	id, found := cache.GetIDBySubjectAndSquema("subject", "schema")
	if id != 1 {
		t.Fail()
	}
	if !found {
		t.Fail()
	}
}

func TestGetIDBySubjectAndSquemaForNewSchema(t *testing.T) {
	cache := NewCacheSchemaRegistry()
	event1 := event1{}
	cache.SetBySubjectSquema(event1.Subject(), event1.AvroSchema(), 1)

	event2 := event2{}
	id, found := cache.GetIDBySubjectAndSquema(event2.Subject(), event2.AvroSchema())
	if id == 1 {
		t.Fail()
	}
	if found {
		t.Fail()
	}
}

type event1 struct {
}

func (event1) AvroSchema() string {
	return `{		
		"type": "record",
		"name": "name",
		"namespace": "namespace",
		"fields": [
			{"name": "uuid", "type": "string"}
		]
	}`
}

func (event1) Subject() string {
	return "name"
}

func (event1) ToStringMap() map[string]interface{} {
	var m map[string]interface{}

	return m
}

func (event1) ID() string {
	return "id"
}

type event2 struct {
}

func (event2) AvroSchema() string {
	return `{		
		"type": "record",
		"name": "name",
		"namespace": "namespace.booking",
		"fields": [
			{"name": "uuid", "type": "string"},
			{"name": "name", "type": "string"}
		]
	}`
}

func (event2) Subject() string {
	return "name"
}

func (event2) ToStringMap() map[string]interface{} {
	var m map[string]interface{}

	return m
}

func (event2) ID() string {
	return "id"
}
