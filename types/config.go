package types

import "time"

type SymbolsCache struct {
	Symbols map[Symbol]Oldest `json:"symbols"`
	Loaded  time.Time         `json:"loaded"`
}

type SQL_MODE int

const (
	SQL_INSERT_MODE SQL_MODE = iota
	SQL_COPY_MODE
)
