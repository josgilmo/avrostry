package avrostry

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

type SchemaRegistryClient interface {
	Register(subject string, schema string /* avro.Schema */) (int32, error)
	GetByID(id int32) (string /* avro.Schema */, error)
	// GetLatestSchemaMetadata(subject string) (*SchemaMetadata, error)
	// GetVersion(subject string, schema avro.Schema) (int32, error)
}

type SchemaMetadata struct {
	Id      int32
	Version int32
	Schema  string
}

type CompatibilityLevel string

const (
	BackwardCompatibilityLevel CompatibilityLevel = "BACKWARD"
	ForwardCompatibilityLevel  CompatibilityLevel = "FORWARD"
	FullCompatibilityLevel     CompatibilityLevel = "FULL"
	NoneCompatibilityLevel     CompatibilityLevel = "NONE"
)

const (
	SCHEMA_REGISTRY_V1_JSON               = "application/vnd.schemaregistry.v1+json"
	SCHEMA_REGISTRY_V1_JSON_WEIGHTED      = "application/vnd.schemaregistry.v1+json"
	SCHEMA_REGISTRY_MOST_SPECIFIC_DEFAULT = "application/vnd.schemaregistry.v1+json"
	SCHEMA_REGISTRY_DEFAULT_JSON          = "application/vnd.schemaregistry+json"
	SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED = "application/vnd.schemaregistry+json qs=0.9"
	JSON                                  = "application/json"
	JSON_WEIGHTED                         = "application/json qs=0.5"
	GENERIC_REQUEST                       = "application/octet-stream"
)

var PREFERRED_RESPONSE_TYPES = []string{SCHEMA_REGISTRY_V1_JSON, SCHEMA_REGISTRY_DEFAULT_JSON, JSON}

type RegisterSchemaResponse struct {
	Id int32
}

type GetSchemaResponse struct {
	Schema string
}

type GetSubjectVersionResponse struct {
	Subject string
	Version int32
	Id      int32
	Schema  string
}

type SchemaRegistryManager struct {
	registryURL         string
	CacheSchemaRegistry *CacheSchemaRegistry
}

type CachedSchemaRegistryClient struct {
	registryURL string
	SchemaCache map[string]map[string]int32
	IdCache     map[int32]string
	//versionCache map[string]map[avro.Schema]int32
}

func NewCachedSchemaRegistryClient(registryURL string) *CachedSchemaRegistryClient {
	return &CachedSchemaRegistryClient{
		registryURL: registryURL,
		SchemaCache: make(map[string]map[string]int32),
		IdCache:     make(map[int32]string),
		//versionCache: make(map[string]map[avro.Schema]int32),
	}
}

func NewSchemaRegistryManager(registryURL string) *SchemaRegistryManager {
	cache := NewCacheSchemaRegistry()
	return &SchemaRegistryManager{
		registryURL:         registryURL,
		CacheSchemaRegistry: cache,
		/*
			SchemaCache: make(map[string]map[string]int32),
			IdCache:     make(map[int32]string),
		*/
		//versionCache: make(map[string]map[avro.Schema]int32),
	}
}

func (schemaRegistryManager *SchemaRegistryManager) Register(subject string, schema string /* avro.Schema */) (int32, error) {
	//var schemaIdMap map[string]int32
	var exists bool

	id, exists := schemaRegistryManager.CacheSchemaRegistry.GetIdBySubjectAndSquema(subject, schema)
	if exists {
		return id, nil
	}
	/*
		if schemaIdMap, exists = schemaRegistryManager.SchemaCache[subject]; !exists {
			schemaIdMap = make(map[string]int32)
			schemaRegistryManager.SchemaCache[subject] = schemaIdMap
		}

		var id int32
		if id, exists = schemaIdMap[schema]; exists {
			return id, nil
		}
	*/

	request, err := schemaRegistryManager.newDefaultRequest("POST",
		fmt.Sprintf(REGISTER_NEW_SCHEMA, subject),
		strings.NewReader(fmt.Sprintf("{\"schema\": %s}", strconv.Quote(schema))))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}

	if schemaRegistryManager.isOK(response) {
		decodedResponse := &RegisterSchemaResponse{}
		if schemaRegistryManager.handleSuccess(response, decodedResponse) != nil {
			return 0, err
		}

		schemaRegistryManager.CacheSchemaRegistry.SetBySubjectSquema(subject, schema, decodedResponse.Id)
		/*
			schemaIdMap[schema] = decodedResponse.Id
			schemaRegistryManager.IdCache[decodedResponse.Id] = schema
		*/

		return decodedResponse.Id, err
	} else {
		return 0, schemaRegistryManager.handleError(response)
	}
}

func (schemaRegistryManager *SchemaRegistryManager) GetByID(id int32) (string, error) {
	var schema string // avro.Schema
	var exists bool
	if schema, exists = schemaRegistryManager.CacheSchemaRegistry.IdCache[id]; exists {
		return schema, nil
	}

	request, err := schemaRegistryManager.newDefaultRequest("GET", fmt.Sprintf(GET_SCHEMA_BY_ID, id), nil)
	if err != nil {
		return "", err
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return "", err
	}

	if schemaRegistryManager.isOK(response) {
		decodedResponse := &GetSchemaResponse{}
		if schemaRegistryManager.handleSuccess(response, decodedResponse) != nil {
			return "", err
		}
		// schema, err := avro.ParseSchema(decodedResponse.Schema)
		schemaRegistryManager.CacheSchemaRegistry.SetSchemaById(id, schema)
		// schemaRegistryManager.IdCache[id] = decodedResponse.Schema //schema.String()

		return decodedResponse.Schema, err //return schema.String(), err
	} else {
		return "", schemaRegistryManager.handleError(response)
	}
}

/*
func (schemaRegistryManager *CachedSchemaRegistryClient) GetLatestSchemaMetadata(subject string) (*SchemaMetadata, error) {
	request, err := schemaRegistryManager.newDefaultRequest("GET", fmt.Sprintf(GET_SPECIFIC_SUBJECT_VERSION, subject, "latest"), nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	if schemaRegistryManager.isOK(response) {
		decodedResponse := &GetSubjectVersionResponse{}
		if schemaRegistryManager.handleSuccess(response, decodedResponse) != nil {
			return nil, err
		}

		return &SchemaMetadata{decodedResponse.Id, decodedResponse.Version, decodedResponse.Schema}, err
	} else {
		return nil, schemaRegistryManager.handleError(response)
	}
}

func (schemaRegistryManager *CachedSchemaRegistryClient) GetVersion(subject string, schema avro.Schema) (int32, error) {
	var schemaVersionMap map[avro.Schema]int32
	var exists bool
	if schemaVersionMap, exists = schemaRegistryManager.versionCache[subject]; !exists {
		schemaVersionMap = make(map[avro.Schema]int32)
		schemaRegistryManager.versionCache[subject] = schemaVersionMap
	}

	var version int32
	if version, exists = schemaVersionMap[schema]; exists {
		return version, nil
	}

	request, err := schemaRegistryManager.newDefaultRequest("POST",
		fmt.Sprintf(CHECK_IS_REGISTERED, subject),
		strings.NewReader(fmt.Sprintf("{\"schema\": %s}", strconv.Quote(schema.String()))))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}

	if schemaRegistryManager.isOK(response) {
		decodedResponse := &GetSubjectVersionResponse{}
		if schemaRegistryManager.handleSuccess(response, decodedResponse) != nil {
			return 0, err
		}
		schemaVersionMap[schema] = decodedResponse.Version

		return decodedResponse.Version, err
	} else {
		return 0, schemaRegistryManager.handleError(response)
	}
}
*/
