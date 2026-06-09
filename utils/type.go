package utils

import "reflect"

func IsNil[T any](v T) bool {
	rv := reflect.ValueOf(v)

	switch rv.Kind() {
	case reflect.Ptr,
		reflect.Interface,
		reflect.Map,
		reflect.Slice,
		reflect.Func,
		reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

func IsNilable[T any](v T) bool {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr,
		reflect.Interface,
		reflect.Map,
		reflect.Slice,
		reflect.Func,
		reflect.Chan:
		return true
	default:
		return false
	}
}
