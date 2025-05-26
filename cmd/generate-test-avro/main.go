package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/hamba/avro/v2"
	"github.com/hamba/avro/v2/ocf"
)

// User represents a simple user record
type User struct {
	ID       int64   `avro:"id"`
	Name     string  `avro:"name"`
	Email    string  `avro:"email"`
	Age      int32   `avro:"age"`
	Active   bool    `avro:"active"`
	Score    float64 `avro:"score"`
	Metadata string  `avro:"metadata"`
}

func main() {
	// Define the Avro schema
	schemaJSON := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "long"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string"},
			{"name": "age", "type": "int"},
			{"name": "active", "type": "boolean"},
			{"name": "score", "type": "double"},
			{"name": "metadata", "type": "string"}
		]
	}`

	// Validate the schema by parsing it
	_, err := avro.Parse(schemaJSON)
	if err != nil {
		log.Fatalf("Failed to parse schema: %v", err)
	}

	// Create output file
	outputFile := "testdata/simple_users.avro"
	if len(os.Args) > 1 {
		outputFile = os.Args[1]
	}

	// Ensure testdata directory exists
	if err := os.MkdirAll("testdata", 0755); err != nil {
		log.Fatalf("Failed to create testdata directory: %v", err)
	}

	file, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create output file: %v", err)
	}
	defer file.Close()

	// Create OCF writer
	writer, err := ocf.NewEncoder(schemaJSON, file, ocf.WithCodec(ocf.Deflate))
	if err != nil {
		log.Fatalf("Failed to create OCF writer: %v", err)
	}
	defer writer.Close()

	// Generate test data
	users := []User{
		{
			ID:       1,
			Name:     "Alice Johnson",
			Email:    "alice@example.com",
			Age:      28,
			Active:   true,
			Score:    95.5,
			Metadata: `{"department": "engineering", "level": "senior"}`,
		},
		{
			ID:       2,
			Name:     "Bob Smith",
			Email:    "bob@example.com",
			Age:      34,
			Active:   true,
			Score:    87.2,
			Metadata: `{"department": "marketing", "level": "manager"}`,
		},
		{
			ID:       3,
			Name:     "Carol Davis",
			Email:    "carol@example.com",
			Age:      29,
			Active:   false,
			Score:    92.8,
			Metadata: `{"department": "sales", "level": "junior"}`,
		},
		{
			ID:       4,
			Name:     "David Wilson",
			Email:    "david@example.com",
			Age:      42,
			Active:   true,
			Score:    78.9,
			Metadata: `{"department": "engineering", "level": "principal"}`,
		},
		{
			ID:       5,
			Name:     "Eve Brown",
			Email:    "eve@example.com",
			Age:      31,
			Active:   true,
			Score:    89.1,
			Metadata: `{"department": "design", "level": "senior"}`,
		},
	}

	// Write records
	for _, user := range users {
		if err := writer.Encode(user); err != nil {
			log.Fatalf("Failed to encode user: %v", err)
		}
	}

	if err := writer.Flush(); err != nil {
		log.Fatalf("Failed to flush writer: %v", err)
	}

	fmt.Printf("Successfully created Avro file: %s\n", outputFile)
	fmt.Printf("Records written: %d\n", len(users))

	// Print schema for reference
	fmt.Println("\nSchema:")
	var prettySchema interface{}
	if err := json.Unmarshal([]byte(schemaJSON), &prettySchema); err == nil {
		if prettyBytes, err := json.MarshalIndent(prettySchema, "", "  "); err == nil {
			fmt.Println(string(prettyBytes))
		}
	}
}
