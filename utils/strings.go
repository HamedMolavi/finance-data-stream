package utils

import (
	"math/rand/v2"
	"strings"
)

type RandomFuncOptions struct {
	Prefix  string
	Postfix string
	Length  int
	Numbers bool
	Letters bool
}

func (opt RandomFuncOptions) GenerateRandomString() string {
	return GenerateRandomString(opt)
}

func GenerateRandomString(opt RandomFuncOptions) string {
	letters := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}
	numbers := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "0"}
	set := make([]string, 0)
	if opt.Letters {
		set = append(set, numbers...)
	}
	if len(set) == 0 || opt.Letters {
		set = append(set, letters...)
	}
	randomString := []string{opt.Prefix}
	if opt.Length == 0 {
		opt.Length = 12
	}
	for range opt.Length {
		randomString = append(randomString, set[rand.IntN(len(set))])
	}
	randomString = append(randomString, opt.Postfix)
	return strings.Join(randomString, "")
}
