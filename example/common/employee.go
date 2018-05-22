package common

import (
	"github.com/linkedin/goavro"
)

type Status string

const (
	Retired  Status = "RETIRED"
	Salary          = "SALARY"
	Hourly          = "HOURLY"
	PartTime        = "PART_TIME"
)

type Employee struct {
	EmployeeID string
	FirstName  string
	LastName   string
	Age        int
	Emails     []string
	Phone      Phone
	Status     Status
}

type Phone struct {
	CountryCode string
	Number      string
}

// AvroSchema for Employee
func (e Employee) AvroSchema() string {
	return `{
		"namespace": "josgilmo.avrostry",
		"type": "record",
		"name": "create_employee",
		"doc" : "Represents an Employee at a company",
		"fields": [
			{"name": "id", "type": "string", "doc": "Employee ID"},
			{"name": "firstName", "type": "string", "doc": "The persons given name"},
			{"name": "lastName", "type": "string"},
			{"name": "age",  "type": "int", "default": 18},
			{"name": "emails", "default":[], "type":{"type": "array", "items": "string"}},
			{"name": "phone", "type": ["null", {
				"namespace": "josgilmo.avrostry",
				"type": "record",
				"name": "phone",
				"fields": [
					{"name": "countryCode", "type": "string", "default" : "34"},
					{"name": "number", "type": "string"}
				]
			}]},
			{"name":"status", "default" :"SALARY", 
				"type": {"type": "enum", 
					"name": "Status", 
					"symbols": ["RETIRED", "SALARY", "HOURLY", "PART_TIME"]
				}
			}
		]
	}`
}

func (Employee) Subject() string {
	return "josgilmo.avrostry.create_employee"
}

func StringMapToEmployee(data map[string]interface{}) *Employee {
	var (
		employee Employee
	)

	for k, v := range data {
		switch k {
		case "id":
			employee.EmployeeID = v.(string)
		case "firstName":
			employee.FirstName = v.(string)
		case "lastName":
			employee.LastName = v.(string)
		case "age":
			employee.Age = int(v.(int32))
		case "emails":
			emails, _ := v.([]interface{})
			for _, email := range emails {
				employee.Emails = append(employee.Emails, email.(string))
			}
		case "phone":
			phone, _ := v.(map[string]interface{})["josgilmo.avrostry.phone"].(map[string]interface{})
			for k, v := range phone {
				switch k {
				case "countryCode":
					employee.Phone.CountryCode = v.(string)
				case "number":
					employee.Phone.Number = v.(string)
				}
			}
		case "status":
			employee.Status = Status(v.(string))
		}
	}

	return &employee
}

func (e Employee) ToStringMap() map[string]interface{} {
	datumIn := map[string]interface{}{
		"id":        e.EmployeeID,
		"firstName": e.FirstName,
		"lastName":  e.LastName,
		"age":       e.Age,
		"emails":    e.Emails,
		"status":    string(e.Status),
	}

	datumIn["phone"] = goavro.Union("josgilmo.avrostry.phone", map[string]interface{}{
		"countryCode": e.Phone.CountryCode,
		"number":      e.Phone.Number,
	})

	return datumIn
}

func (e Employee) ID() string {
	return e.EmployeeID
}
