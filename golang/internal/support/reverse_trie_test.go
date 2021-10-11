package support

import (
	"golang/internal/comparable"
	"testing"
)


func TestReverseTrie(t *testing.T) {
	t.Run("Creating a trie, adding keys, recovering values", func(t *testing.T) {
		value1 := NewMonotonicByte(7)
		value2 := NewMonotonicByte(8)
		value3 := NewMonotonicByte(9)

		trie := NewReverseTrie(monotonicByteCtor)
		oldValue, err := trie.Add("Path", value1)
		assertNoError(t, err)
		assertNil(t, oldValue)

		oldValue, err = trie.Add("Other path", value2)
		assertNoError(t, err)
		assertNil(t, oldValue)

		oldValue, err = trie.Add("Path", value3)
		assertNoError(t, err)
		assertEqual(t, oldValue, value1)

		value, found, err := trie.Get("Other path")
		assertNoError(t,err)
		assertTrue(t,found,"key found")
		assertEqual(t, value, value2)


		value, found, err = trie.Get("Path")
		assertNoError(t,err)
		assertTrue(t,found,"key found")
		assertEqual(t, value, value3)
	})

	t.Run("Deleting, and Searching from prefix", func(t *testing.T) {
		value := NewMonotonicByte(7)
		trie := NewReverseTrie(monotonicByteCtor)

		_, _ = trie.Add("hello, world!", value)
		_, _ = trie.Add("hello, country?", value)
		_, _ = trie.Add("hello, local vicinity", value)
		_, _ = trie.Add("goodbye", value)

		trie.Delete("hello, country?")

		result := trie.Search("hello")
		assertContains(t, result, "hello, world!", "hello, local vicinity")
		assertNotContains(t, result, "goodbye")
	})

	t.Run("Freezing and Defrosting", func(t *testing.T) {
		// Build up a trie
		value1,_ := NewRandomId()
		value2,_ := NewRandomId()
		value3,_ := NewRandomId()
		original := NewReverseTrie(serialIdCtor)

		_, _ = original.Add("If you can keep your head when all about you", value1)
		_, _ = original.Add("If you can trust yourself when all men doubt you", value2)
		_, _ = original.Add("If you can wait and not be tired by waiting", value3)
		_, _ = original.Add("goodbye", value1)

		original.Delete("goodbye")

		// Freeze and restore
		rdr := original.Freeze()

		restored := NewReverseTrie(serialIdCtor)
		err := restored.Defrost(rdr)
		assertNoError(t, err)

		// Check all the contents are still correct
		searchResult := restored.Search("If")
		assertContains(t, searchResult, "If you can keep your head when all about you", "If you can trust yourself when all men doubt you","If you can wait and not be tired by waiting")
		assertNotContains(t, searchResult, "goodbye")

		value, found, err := restored.Get("If you can keep your head when all about you")
		assertNoError(t,err)
		assertTrue(t, found, "found 1")
		assertEqual(t, value, value1)

		value, found, err = restored.Get("If you can trust yourself when all men doubt you")
		assertNoError(t,err)
		assertTrue(t, found, "found 2")
		assertEqual(t, value, value2)

		value, found, err = restored.Get("If you can wait and not be tired by waiting")
		assertNoError(t,err)
		assertTrue(t, found, "found 3")
		assertEqual(t, value, value3)

		value, found, err = restored.Get("If you can pick your nose")
		assertNoError(t,err)
		assertFalse(t, found, "found 4")
	})
}


//<editor-fold desc="Helper functions">
func monotonicByteCtor()SerialComparable{return NewMonotonicByte(0) }
func serialIdCtor()SerialComparable{return NewZeroId() }

func assertNotContains(t *testing.T, result []string, contents ...string) {
	for _, content := range contents {
		if containsSingle(result, content) {
			t.Errorf("Expected NOT to find '%v', but it was present in %v", content, result)
		}
	}
}

func assertContains(t *testing.T, result []string, contents ...string) {
	for _, content := range contents {
		if !containsSingle(result, content) {
			t.Errorf("Expected to find '%v', but it was missing from %v", content, result)
		}
	}
}

func containsSingle(result []string, content string) bool {
	for _, str := range result {
		if str == content {return true}
	}
	return false
}

func assertEqual(t *testing.T, actual comparable.Comparable, expected comparable.Comparable) {
	if !comparable.Is(actual).EqualTo(expected) {
		t.Errorf("Expected '%v', but got '%v'", expected, actual)
	}
}

func assertNil(t *testing.T, value interface{}) {
	if value != nil {
		t.Errorf("Expected nil but got %v", value)
	}
}
//</editor-fold>
