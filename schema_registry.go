package avrostry

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

// SchemaRegistryClient Interface for manage Schema Registry
type SchemaRegistryClient interface {
	Register(subject, schema string) (id int32, err error)
	GetByID(id int32) (schema string, err error)
}

// SchemaMetadata Metainformation about Schemas
type SchemaMetadata struct {
	ID      int32
	Version int32
	Schema  string
}

// CompatibilityLevel Schema Registry compatibility level
type CompatibilityLevel string

const (
	BackwardCompatibilityLevel CompatibilityLevel = "BACKWARD"
	ForwardCompatibilityLevel  CompatibilityLevel = "FORWARD"
	FullCompatibilityLevel     CompatibilityLevel = "FULL"
	NoneCompatibilityLevel     CompatibilityLevel = "NONE"
)

const (
	GetSchemaByID             = "/schemas/ids/%d"
	GetSubjects               = "/subjects"
	GetSubjectVersions        = "/subjects/%s/versions"
	GetSpecificSubjectVersion = "/subjects/%s/versions/%s"
	RegisterNewSchema         = "/subjects/%s/versions"
	CheckIsRegistered         = "/subjects/%s"
	TestCompatibility         = "/compatibility/subjects/%s/versions/%s"
	Config                    = "/config"
)

type ErrorMessage struct {
	Code    int32  `json:"error_code"`
	Message string `json:"message"`
}

func (err *ErrorMessage) Error() string {
	return fmt.Sprintf("%d: %s ", err.Code, err.Message)
}

// RegisterSchemaResponse ID Schema response
type RegisterSchemaResponse struct {
	ID int32 `json:"id"`
}

// GetSchemaResponse Schema string response
type GetSchemaResponse struct {
	Schema string `json:"schema"`
}

type HttpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// SchemaRegistryManager Client and cache schema registry
type SchemaRegistryManager struct {
	registryURL string
	cache       *CacheSchemaRegistry
	httpDoer    HttpDoer
}

// NewSchemaRegistryManager SchemaRegistryManager Constructor
func NewSchemaRegistryManager(registryURL string, cache *CacheSchemaRegistry, httpDoer HttpDoer) *SchemaRegistryManager {
	return &SchemaRegistryManager{
		registryURL: strings.TrimRight(registryURL, "/"),
		cache:       cache,
		httpDoer:    httpDoer,
	}
}

// Register set a subject schema in Schema Registry if there is not in cache.
func (srm *SchemaRegistryManager) Register(subject string, schema string) (int32, error) {
	id, exists := srm.cache.GetIDBySubjectAndSquema(subject, schema)
	if exists {
		return id, nil
	}

	request, err := srm.newDefaultRequest("POST",
		fmt.Sprintf(RegisterNewSchema, subject),
		strings.NewReader(fmt.Sprintf(`{"schema": %s}`, strconv.Quote(schema))))

	response, err := srm.httpDoer.Do(request)
	if err != nil {
		return 0, err
	}

	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, err
	}

	if !isOK(response.StatusCode) {
		return 0, newError(body)
	}

	var decodedResponse RegisterSchemaResponse
	err = json.Unmarshal(body, &decodedResponse)
	if err != nil {
		return 0, err
	}
	srm.cache.SetBySubjectSquema(subject, schema, decodedResponse.ID)
	return decodedResponse.ID, err
}

// GetByID given an id, retrieve the related Schema from Kafka Schema Registry
func (srm *SchemaRegistryManager) GetByID(id int32) (string, error) {
	schema, exists := srm.cache.GetByID(id)
	if exists {
		return schema, nil
	}

	request, err := srm.newDefaultRequest("GET", fmt.Sprintf(GetSchemaByID, id), nil)
	if err != nil {
		return "", err
	}

	response, err := srm.httpDoer.Do(request)
	if err != nil {
		return "", err
	}

	defer response.Body.Close()

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	if !isOK(response.StatusCode) {
		return "", newError(body)
	}

	var decodedResponse GetSchemaResponse
	err = json.Unmarshal(body, &decodedResponse)
	if err != nil {
		return "", err
	}
	srm.cache.SetSchemaByID(id, decodedResponse.Schema)
	return decodedResponse.Schema, err
}

func (srm *SchemaRegistryManager) newDefaultRequest(method string, uri string, reader io.Reader) (*http.Request, error) {
	request, err := http.NewRequest(method, srm.registryURL+uri, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	return request, nil
}

func isOK(statusCode int) bool {
	return statusCode >= 200 && statusCode < 300
}

func newError(respBody []byte) error {
	var registryError ErrorMessage
	err := json.Unmarshal(respBody, &registryError)
	if err != nil {
		return err
	}
	return &registryError
}
