package mr

import (
	"sync"
)

// MutexMap is a thread-safe map with a generic key type.
type MutexMap[K comparable, V any] struct {
	mu    sync.Mutex
	store map[K]V
}

// NewMutexMap creates a new thread-safe map.
func NewMutexMap[K comparable, V any]() *MutexMap[K, V] {
	return &MutexMap[K, V]{store: make(map[K]V)}
}

// Set sets the value for the given key.
func (m *MutexMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.store[key] = value
}

// Get gets the value associated with the given key.
func (m *MutexMap[K, V]) Get(key K) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	value, exists := m.store[key]
	return value, exists
}

// Delete deletes the entry for the given key.
func (m *MutexMap[K, V]) Delete(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, key)
}

// Len returns the number of entries in the map.
func (m *MutexMap[K, V]) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.store)
}
