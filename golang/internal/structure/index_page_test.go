package structure

import (
	"golang/internal/support"
	"testing"
)

func TestIndexPage(t *testing.T) {
	t.Run("Manipulating indexes", func(t *testing.T) {
		page := NewIndexPage()

		did0, _ := support.NewRandomId();
		did1, _ := support.NewRandomId(); pid1 := 10
		did2, _ := support.NewRandomId(); pid2 := 10
		did3, _ := support.NewRandomId(); pid3 := 10

		// Inserting things
		ok, err := page.TryInsert(did1, pid1)
		assertNoError(t, err); assertTrue(t, ok, "ins 1")

		ok, err = page.TryInsert(did2, pid2)
		assertNoError(t, err); assertTrue(t, ok, "ins 2")

		ok, err = page.TryInsert(did3, pid3)
		assertNoError(t, err); assertTrue(t, ok, "ins 3")

		// Finding things
		link, found := page.Search(did3)
		assertTrue(t, found, "find 1")
		ok, pageId := link.TryGetLink(0)
		assertEqualInt(t, pageId, pid3 )

		link, found = page.Search(did1)
		assertTrue(t, found, "find 2")
		ok, pageId = link.TryGetLink(0)
		assertEqualInt(t, pageId, pid1 )

		link, found = page.Search(did0)
		assertFalse(t, found, "find 3")

		// Remove and try to find
		ok = page.Remove(did1)
		assertTrue(t, ok, "remove")

		link, found = page.Search(did1)
		assertTrue(t, found, "find 4") // the index is still there, it just points at a 'bad' page
		ok, pageId = link.TryGetLink(0)
		assertFalse(t, ok, "find 4 link")
	})

	t.Run("Freeze and defrost", func(t *testing.T) {
		original := NewIndexPage()

		did1, _ := support.NewRandomId(); pid1 := 10
		did2, _ := support.NewRandomId(); pid2 := 10
		did3, _ := support.NewRandomId(); pid3 := 10

		// Inserting things
		_, _ = original.TryInsert(did1, pid1)
		_, _ = original.TryInsert(did2, pid2)
		_, _ = original.TryInsert(did3, pid3)

		rdr := original.Freeze()
		original = nil
		restored := NewIndexPage()
		err := restored.Defrost(rdr)
		assertNoError(t,err)

		// Check the values came back
		link, found := restored.Search(did1)
		assertTrue(t, found, "find 1")
		_, pageId := link.TryGetLink(0)
		assertEqualInt(t, pageId, pid1)

		link, found = restored.Search(did2)
		assertTrue(t, found, "find 2")
		_, pageId = link.TryGetLink(0)
		assertEqualInt(t, pageId, pid2)

		link, found = restored.Search(did3)
		assertTrue(t, found, "find 3")
		_, pageId = link.TryGetLink(0)
		assertEqualInt(t, pageId, pid3)
	})
}

//<editor-fold desc="Helper functions">
func assertEqualInt(t *testing.T, actual int, expected int) {
	if actual != expected {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}
//</editor-fold>
