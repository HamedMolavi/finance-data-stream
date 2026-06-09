package pipeline

import "github.com/google/uuid"

// Operation options
type StageOption func(*StageConfig)

type StageConfig struct {
	ID       string
	Name     string
	DropNil  bool
	TrySync  bool
	Capacity int
}

// Default values
func defaultConfig() *StageConfig {
	return &StageConfig{
		ID:      uuid.New().String(),
		Name:    "no name",
		DropNil: false,
		TrySync: false,
	}
}
func CreateConfig(opts []StageOption) *StageConfig {
	cfg := defaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// Option constructors
func WithCapacity(ca int) StageOption {
	return func(c *StageConfig) {
		c.Capacity = ca
	}
}

// Option constructors
func WithDropNil() StageOption {
	return func(c *StageConfig) {
		c.DropNil = true
	}
}
func WithTrySync() StageOption {
	return func(c *StageConfig) {
		c.TrySync = true
	}
}
func WithName(n string) StageOption {
	return func(c *StageConfig) {
		c.Name = n
	}
}
func WithID(id string) StageOption {
	return func(c *StageConfig) {
		c.ID = id
	}
}
