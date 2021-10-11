package structure

import "testing"

func TestFreeListPage_TryGetNext(t *testing.T) {
	t.Run("add and remove", func(t *testing.T) {
		page := NewFreeListPage()

		// Should be empty
		next, found := page.TryGetNext()
		assertFalse(t, found, "found 1")
		assertEqualInt32(t, next, -1)

		// Add some pages
		ok := page.TryAdd(11)
		assertTrue(t, ok, "add 1")
		ok = page.TryAdd(12)
		assertTrue(t, ok, "add 2")
		ok = page.TryAdd(11) // should be able to repeat
		assertTrue(t, ok, "add 3")

		// recover those pages
		next, found = page.TryGetNext()
		assertTrue(t, found, "found 2")
		assertNotZero(t, next, "found 2")

		next, found = page.TryGetNext()
		assertTrue(t, found, "found 3")
		assertNotZero(t, next, "found 3")

		// should be empty again
		next, found = page.TryGetNext()
		assertFalse(t, found, "found 4")
		assertEqualInt32(t, next, -1)
	})

	t.Run("freeze and defrost", func(t *testing.T) {
		original := NewFreeListPage()
		original.TryAdd(11)
		original.TryAdd(110)
		original.TryAdd(1010)
		original.TryAdd(10010)

		rdr := original.Freeze()
		original = nil

		restored := NewFreeListPage()
		err := restored.Defrost(rdr)
		assertNoError(t, err)

		next, found := restored.TryGetNext()
		assertPageRecovered(t, found, next, 11)

		next, found = restored.TryGetNext()
		assertPageRecovered(t, found, next, 110)

		next, found = restored.TryGetNext()
		assertPageRecovered(t, found, next, 1010)

		next, found = restored.TryGetNext()
		assertPageRecovered(t, found, next, 10010)

		// should be empty again
		next, found = restored.TryGetNext()
		assertFalse(t, found, "found 4")
		assertEqualInt32(t, next, -1)
	})
}

//<editor-fold desc="Helper functions">
func assertPageRecovered(t *testing.T, found bool, actual int32, expected int32) {
	if !found {
		t.Errorf("Expected to find a page, but did not")
	}
	if actual != expected {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func assertNotZero(t *testing.T, value int32, msg string) {
	if value == 0 {
		t.Errorf("Expected non-zero, but got zero; %v",msg)
	}
}
//</editor-fold>
