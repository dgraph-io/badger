package badger

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestOptions(t *testing.T) {
	// copy all the default options over to a big SuperFlag string
	defaultSuperFlag := generateSuperFlag(DefaultOptions(""))
	// fill an empty Options with values from the SuperFlag
	overwritten := overwriteOptions(defaultSuperFlag, Options{})
	// make sure they're equal
	if !optionsEqual(DefaultOptions(""), overwritten) {
		t.Fatal("generated default SuperFlag != default Options")
	}
}

// generateSuperFlag generates an identical SuperFlag string from the provided
// Options--useful for testing.
func generateSuperFlag(options Options) string {
	superflag := ""
	v := reflect.ValueOf(&options).Elem()
	optionsStruct := v.Type()
	for i := 0; i < v.NumField(); i++ {
		if field := v.Field(i); field.CanInterface() {
			name := strings.ToLower(optionsStruct.Field(i).Name)
			kind := v.Field(i).Kind()
			switch kind {
			case reflect.Bool:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.Bool())
			case reflect.Int, reflect.Int64:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.Int())
			case reflect.Uint32, reflect.Uint64:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.Uint())
			case reflect.Float64:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.Float())
			case reflect.String:
				superflag += name + "="
				superflag += fmt.Sprintf("%v; ", field.String())
			default:
				continue
			}
		}
	}
	return superflag
}

// optionsEqual just compares the values of two Options structs
func optionsEqual(o1, o2 Options) bool {
	o1v := reflect.ValueOf(&o1).Elem()
	o2v := reflect.ValueOf(&o2).Elem()
	for i := 0; i < o1v.NumField(); i++ {
		if o1v.Field(i).CanInterface() {
			kind := o1v.Field(i).Kind()
			// compare values
			switch kind {
			case reflect.Bool:
				if o1v.Field(i).Bool() != o2v.Field(i).Bool() {
					return false
				}
			case reflect.Int, reflect.Int64:
				if o1v.Field(i).Int() != o2v.Field(i).Int() {
					return false
				}
			case reflect.Uint32, reflect.Uint64:
				if o1v.Field(i).Uint() != o2v.Field(i).Uint() {
					return false
				}
			case reflect.Float64:
				if o1v.Field(i).Float() != o2v.Field(i).Float() {
					return false
				}
			case reflect.String:
				if o1v.Field(i).String() != o2v.Field(i).String() {
					return false
				}
			}
		}
	}
	return true
}
