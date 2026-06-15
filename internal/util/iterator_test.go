package util

import "kora/internal/engine"

// compile-time check that a concrete iterator can satisfy the util.Iterator contract.
type testIterator struct{}

func (testIterator) Next() bool                         { return false }
func (testIterator) Current() *engine.Entry             { return nil }
func (testIterator) Seek(target []byte) bool             { return false }
func (testIterator) Lock()                               {}
func (testIterator) Unlock()                             {}
func (testIterator) Close() error                        { return nil }

var _ Iterator = testIterator{}
