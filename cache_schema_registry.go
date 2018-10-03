package avrostry

import (
	"sync"
)

// CacheSchemaRegistry Struct for storage Schema Registry information
type CacheSchemaRegistry struct {
	sync.RWMutex
	//
	schemaCache map[string]map[string]int32 // subject => schema => id
	idCache     map[int32]string            // id => schema
}

// NewCacheSchemaRegistry CacheSchemaRegistry constructor.
func NewCacheSchemaRegistry() *CacheSchemaRegistry {
	return &CacheSchemaRegistry{
		schemaCache: make(map[string]map[string]int32),
		idCache:     make(map[int32]string),
	}
}

// GetByID Get the schema string given his id
func (cache *CacheSchemaRegistry) GetByID(id int32) (string, bool) {
	cache.RLock()
	schema, ok := cache.idCache[id]
	cache.RUnlock()
	return schema, ok
}

// SetBySubjectSquema Store in cache a schema id related to the pair <subject, squema>
func (cache *CacheSchemaRegistry) SetBySubjectSquema(subject, schema string, id int32) {
	cache.Lock()
	cache.idCache[id] = schema
	cache.schemaCache[subject] = map[string]int32{schema: id}
	cache.Unlock()
}

// SetSchemaByID Storage the schema hashed by it´s id.
func (cache *CacheSchemaRegistry) SetSchemaByID(id int32, schema string) {
	cache.Lock()
	cache.idCache[id] = schema
	cache.Unlock()
}

// GetIDBySubjectAndSquema Retrieve the schema id, given the subject and schema.
func (cache *CacheSchemaRegistry) GetIDBySubjectAndSquema(subject, schema string) (int32, bool) {
	cache.RLock()
	schemaIDMap, existsSubject := cache.schemaCache[subject]
	if !existsSubject {
		cache.RUnlock()
		cache.Lock()
		// We have to double check here because more than one go routine could find exists=false
		schemaIDMap, existsSubject = cache.schemaCache[subject]
		cache.Unlock()
		cache.RLock()
	}
	var id int32
	var exists bool

	if existsSubject {
		id, exists = schemaIDMap[schema]
	}

	cache.RUnlock()
	return id, exists
}
