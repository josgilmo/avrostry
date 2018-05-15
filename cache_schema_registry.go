package avrostry

type CacheSchemaRegistry struct {
	SchemaCache map[string]map[string]int32
	IdCache     map[int32]string
	// TODO: Implement cache for Versions.
	// versionCache map[string]map[avro.Schema]int32
}

func NewCacheSchemaRegistry() *CacheSchemaRegistry {
	return &CacheSchemaRegistry{
		SchemaCache: make(map[string]map[string]int32),
		IdCache:     make(map[int32]string),
	}
	//versionCache: make(map[string]map[avro.Schema]int32),

}

func (cache *CacheSchemaRegistry) GetById(id int32) (string, bool) {
	schema, ok := cache.IdCache[id]

	return schema, ok
}

func (cache *CacheSchemaRegistry) SetBySubjectSquema(subject, schema string, id int32) {
	m := make(map[string]int32)
	m[schema] = id
	cache.IdCache[id] = schema
	cache.SchemaCache[subject] = m
}

func (cache *CacheSchemaRegistry) SetSchemaById(id int32, schema string) {
	cache.IdCache[id] = schema
}
func (cache *CacheSchemaRegistry) GetIdBySubjectAndSquema(subject, schema string) (int32, bool) {
	var schemaIdMap map[string]int32
	var exists bool
	if schemaIdMap, exists = cache.SchemaCache[subject]; !exists {
		schemaIdMap = make(map[string]int32)
		cache.SchemaCache[subject] = schemaIdMap
	}

	var id int32
	if id, exists = schemaIdMap[schema]; exists {
		return id, true
	}

	return 0, false
}
