/*
Copyright 2024 The KEDA Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scalersconfig

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	kerrors "k8s.io/apimachinery/pkg/util/errors"
)

// ParsingOrder is a type that represents the order in which the parameters are parsed
type ParsingOrder string

// Constants that represent the order in which the parameters are parsed
const (
	TriggerMetadata ParsingOrder = "triggerMetadata"
	ResolvedEnv     ParsingOrder = "resolvedEnv"
	AuthParams      ParsingOrder = "authParams"
)

// separators for field tag structure
// e.g. name=stringVal,parsingOrder=triggerMetadata;resolvedEnv;authParams,optional=true
const (
	tagSeparator      = ","
	tagKeySeparator   = "="
	tagValueSeparator = ";"
)

// separators for map and slice elements
const (
	elemSeparator       = ","
	elemKeyValSeparator = "="
)

// field tag parameters
const (
	optionalTag     = "optional"
	deprecatedTag   = "deprecated"
	defaultTag      = "default"
	parsingOrderTag = "parsingOrder"
	nameTag         = "name"
)

// Params is a struct that represents the parameter list that can be used in the keda tag
type Params struct {
	Name         string
	Optional     bool
	ParsingOrder []ParsingOrder
	Default      string
	Deprecated   string
}

// IsDeprecated is a function that returns true if the parameter is deprecated
func (p Params) IsDeprecated() bool {
	return p.Deprecated != ""
}

// DeprecatedMessage is a function that returns the optional deprecated message if the parameter is deprecated
func (p Params) DeprecatedMessage() string {
	if p.Deprecated == deprecatedTag {
		return ""
	}
	return fmt.Sprintf(": %s", p.Deprecated)
}

// TypedConfig is a function that is used to unmarshal the TriggerMetadata, ResolvedEnv and AuthParams
// populating the provided typedConfig where structure fields along with complementary field tags define
// declaratively the parsing rules
func (sc *ScalerConfig) TypedConfig(typedConfig any) error {
	t := reflect.TypeOf(typedConfig)
	if t.Kind() != reflect.Pointer {
		return fmt.Errorf("typedConfig must be a pointer")
	}
	t = t.Elem()
	v := reflect.ValueOf(typedConfig).Elem()

	errors := []error{}
	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i)
		fieldValue := v.Field(i)
		tag := fieldType.Tag.Get("keda")
		if tag == "" {
			continue
		}
		tagParams, err := paramsFromTag(tag, fieldType)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		if err := sc.setValue(fieldValue, tagParams); err != nil {
			errors = append(errors, err)
		}
	}
	return kerrors.NewAggregate(errors)
}

// setValue is a function that sets the value of the field based on the provided params
func (sc *ScalerConfig) setValue(field reflect.Value, params Params) error {
	valFromConfig, exists := sc.configParamValue(params)
	if exists && params.IsDeprecated() {
		return fmt.Errorf("parameter %q is deprecated%v", params.Name, params.DeprecatedMessage())
	}
	if !exists && params.Default != "" {
		exists = true
		valFromConfig = params.Default
	}
	if !exists && (params.Optional || params.IsDeprecated()) {
		return nil
	}
	if !exists && !(params.Optional || params.IsDeprecated()) {
		return fmt.Errorf("missing required parameter %q in %v", params.Name, params.ParsingOrder)
	}
	if err := setConfigValueHelper(valFromConfig, field); err != nil {
		return fmt.Errorf("unable to set param %q value %q: %w", params.Name, valFromConfig, err)
	}
	return nil
}

// setParamValueHelper is a function that sets the value of the parameter
func setConfigValueHelper(valFromConfig string, field reflect.Value) error {
	paramValue := reflect.ValueOf(valFromConfig)
	if paramValue.Type().AssignableTo(field.Type()) {
		field.SetString(valFromConfig)
		return nil
	}
	if paramValue.Type().ConvertibleTo(field.Type()) {
		field.Set(paramValue.Convert(field.Type()))
		return nil
	}
	if field.Kind() == reflect.Map {
		field.Set(reflect.MakeMap(reflect.MapOf(field.Type().Key(), field.Type().Elem())))
		split := strings.Split(valFromConfig, elemSeparator)
		for _, s := range split {
			s := strings.TrimSpace(s)
			kv := strings.Split(s, elemKeyValSeparator)
			if len(kv) != 2 {
				return fmt.Errorf("expected format key%vvalue, got %q", elemKeyValSeparator, s)
			}
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])
			ifcKeyElem := reflect.New(field.Type().Key()).Elem()
			if err := setConfigValueHelper(key, ifcKeyElem); err != nil {
				return fmt.Errorf("map key %q: %w", key, err)
			}
			ifcValueElem := reflect.New(field.Type().Elem()).Elem()
			if err := setConfigValueHelper(val, ifcValueElem); err != nil {
				return fmt.Errorf("map key %q, value %q: %w", key, val, err)
			}
			field.SetMapIndex(ifcKeyElem, ifcValueElem)
		}
		return nil
	}
	if field.Kind() == reflect.Slice {
		elemIfc := reflect.New(field.Type().Elem()).Interface()
		split := strings.Split(valFromConfig, elemSeparator)
		for i, s := range split {
			s := strings.TrimSpace(s)
			if err := setConfigValueHelper(s, reflect.ValueOf(elemIfc).Elem()); err != nil {
				return fmt.Errorf("slice element %d: %w", i, err)
			}
			field.Set(reflect.Append(field, reflect.ValueOf(elemIfc).Elem()))
		}
		return nil
	}
	if field.CanInterface() {
		ifc := reflect.New(field.Type()).Interface()
		if err := json.Unmarshal([]byte(valFromConfig), &ifc); err != nil {
			return fmt.Errorf("unable to unmarshal to field type %v: %w", field.Type(), err)
		}
		field.Set(reflect.ValueOf(ifc).Elem())
		return nil
	}
	return fmt.Errorf("unable to find matching parser for field type %v", field.Type())
}

// configParamValue is a function that returns the value of the parameter based on the parsing order
func (sc *ScalerConfig) configParamValue(params Params) (string, bool) {
	for _, po := range params.ParsingOrder {
		var m map[string]string
		key := params.Name
		switch po {
		case TriggerMetadata:
			m = sc.TriggerMetadata
		case AuthParams:
			m = sc.AuthParams
		case ResolvedEnv:
			m = sc.ResolvedEnv
			key = sc.TriggerMetadata[fmt.Sprintf("%sFromEnv", params.Name)]
		default:
			m = sc.TriggerMetadata
		}
		if param, ok := m[key]; ok && param != "" {
			return param, true
		}
	}
	return "", false
}

// paramsFromTag is a function that returns the Params struct based on the field tag
func paramsFromTag(tag string, field reflect.StructField) (Params, error) {
	params := Params{Name: field.Name}
	tagSplit := strings.Split(tag, tagSeparator)
	for _, ts := range tagSplit {
		tsplit := strings.Split(ts, tagKeySeparator)
		tsplit[0] = strings.TrimSpace(tsplit[0])
		switch tsplit[0] {
		case optionalTag:
			if len(tsplit) == 1 {
				params.Optional = true
			}
			if len(tsplit) > 1 {
				params.Optional = strings.TrimSpace(tsplit[1]) == "true"
			}
		case parsingOrderTag:
			if len(tsplit) > 1 {
				parsingOrder := strings.Split(tsplit[1], tagValueSeparator)
				for _, po := range parsingOrder {
					poTyped := ParsingOrder(strings.TrimSpace(po))
					params.ParsingOrder = append(params.ParsingOrder, poTyped)
				}
			}
		case nameTag:
			if len(tsplit) > 1 {
				params.Name = strings.TrimSpace(tsplit[1])
			}
		case deprecatedTag:
			if len(tsplit) == 1 {
				params.Deprecated = deprecatedTag
			} else {
				params.Deprecated = strings.TrimSpace(tsplit[1])
			}
		case defaultTag:
			if len(tsplit) > 1 {
				params.Default = strings.TrimSpace(tsplit[1])
			}
		default:
			return params, fmt.Errorf("unknown tag %s: %s", tsplit[0], tag)
		}
	}
	return params, nil
}
