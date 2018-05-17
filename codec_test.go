package avrostry

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type Word struct {
	Word string
}

func (word Word) AvroSchema() string {
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

func StringMapToWord(data map[string]interface{}) *Word {
	return &Word{Word: data["Word"].(string)}
}

func (word Word) ToStringMap() map[string]interface{} {
	return map[string]interface{}{
		"Word": word.Word,
	}
}

func (word Word) Subject() string {
	return "words"
}

func (word Word) ID() string {
	return "1"
}

func TestAvroKafkaEncoderDecoder(t *testing.T) {
	word := Word{Word: "Palabro"}

	cache := NewCacheSchemaRegistry()

	// Populate cache so that we don't hit the schemaRegistry
	cache.SetSchemaByID(1, word.AvroSchema())
	cache.SetBySubjectSquema(word.Subject(), word.AvroSchema(), 1)

	manager := NewSchemaRegistryManager("invalidUrl", cache, http.DefaultClient)

	codec := NewKafkaAvroCodec(manager, NewCacheCodec())

	bytes, err := codec.Encode(word)
	require.Nil(t, err)
	require.NotNil(t, bytes)

	subject, event, err := codec.Decode(bytes)
	require.Nil(t, err)
	require.NotNil(t, event)
	require.Equal(t, subject, Word{}.Subject(), "should be equal")

	data, ok := event.(map[string]interface{})
	require.True(t, ok)
	wordDecoded := StringMapToWord(data)
	require.Equal(t, word.Word, wordDecoded.Word, "should be equal")
}
