package avrostry

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const (
	GET_SCHEMA_BY_ID             = "/schemas/ids/%d"
	GET_SUBJECTS                 = "/subjects"
	GET_SUBJECT_VERSIONS         = "/subjects/%s/versions"
	GET_SPECIFIC_SUBJECT_VERSION = "/subjects/%s/versions/%s"
	REGISTER_NEW_SCHEMA          = "/subjects/%s/versions"
	CHECK_IS_REGISTERED          = "/subjects/%s"
	TEST_COMPATIBILITY           = "/compatibility/subjects/%s/versions/%s"
	CONFIG                       = "/config"
)

// ErrorMessage struct for manage errors.
type ErrorMessage struct {
	ErrorCode int32
	Message   string
}

func (err *ErrorMessage) Error() string {
	return fmt.Sprintf("%s(error code: %d)", err.Message, err.ErrorCode)
}

// RegisterSchemaResponse ID Schema response
type RegisterSchemaResponse struct {
	ID int32
}

// GetSchemaResponse Schema string response
type GetSchemaResponse struct {
	Schema string
}

// GetSubjectVersionResponse Schema Version Response
type GetSubjectVersionResponse struct {
	Subject string
	Version int32
	ID      int32
	Schema  string
}

func (schemaRegistryManager *SchemaRegistryManager) newDefaultRequest(method string, uri string, reader io.Reader) (*http.Request, error) {
	url := fmt.Sprintf("%s%s", schemaRegistryManager.registryURL, uri)
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", SCHEMA_REGISTRY_V1_JSON)
	return request, nil
}

func (schemaRegistryManager *SchemaRegistryManager) isOK(response *http.Response) bool {
	return response.StatusCode >= 200 && response.StatusCode < 300
}

func (schemaRegistryManager *SchemaRegistryManager) handleSuccess(response *http.Response, model interface{}) error {
	responseBytes := make([]byte, response.ContentLength)
	response.Body.Read(responseBytes)
	return json.Unmarshal(responseBytes, model)
}

func (schemaRegistryManager *SchemaRegistryManager) handleError(response *http.Response) error {
	registryError := &ErrorMessage{}
	responseBytes := make([]byte, response.ContentLength)
	response.Body.Read(responseBytes)
	err := json.Unmarshal(responseBytes, registryError)
	if err != nil {
		return err
	}

	return registryError
}
