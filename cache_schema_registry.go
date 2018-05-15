package avrostry

// CacheSchemaRegistry Struct for storage Schema Registry information
type CacheSchemaRegistry struct {
	SchemaCache map[string]map[string]int32
	IDCache     map[int32]string
	// TODO: Implement cache for Versions.
	// TODO: Cache the linkedin.Codec relative with every schema.
	// versionCache map[string]map[avro.Schema]int32
}

// NewCacheSchemaRegistry CacheSchemaRegistry constructor.
func NewCacheSchemaRegistry() *CacheSchemaRegistry {
	return &CacheSchemaRegistry{
		SchemaCache: make(map[string]map[string]int32),
		IDCache:     make(map[int32]string),
		//versionCache: make(map[string]map[avro.Schema]int32),
	}

}

// GetByID Get the schema string given his id
func (cache *CacheSchemaRegistry) GetByID(id int32) (string, bool) {
	schema, ok := cache.IDCache[id]

	return schema, ok
}

// SetBySubjectSquema Store in cache a schmea id related to the pair <subject, squema>
func (cache *CacheSchemaRegistry) SetBySubjectSquema(subject, schema string, id int32) {
	m := make(map[string]int32)
	m[schema] = id
	cache.IDCache[id] = schema
	cache.SchemaCache[subject] = m
}

// SetSchemaByID Storage the schema hashed by it´s id.
func (cache *CacheSchemaRegistry) SetSchemaByID(id int32, schema string) {
	cache.IDCache[id] = schema
}

// GetIDBySubjectAndSquema Retrieve the schema id, given the subject and schema.
func (cache *CacheSchemaRegistry) GetIDBySubjectAndSquema(subject, schema string) (int32, bool) {
	var schemaIDMap map[string]int32
	var exists bool
	if schemaIDMap, exists = cache.SchemaCache[subject]; !exists {
		schemaIDMap = make(map[string]int32)
		cache.SchemaCache[subject] = schemaIDMap
	}

	var id int32
	if id, exists = schemaIDMap[schema]; exists {
		return id, true
	}

	return 0, false
}
