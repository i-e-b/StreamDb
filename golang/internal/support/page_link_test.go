package support

import (
	"sync"
	"testing"
)

func TestPageLink(t *testing.T) {
	t.Run("Page link can be serialised and restored", func(t *testing.T) {
		subject := NewVersionedLink()
		for i := 0; i < 10; i++ {
			subject.WriteNewLink(i)
		}

		// Check the versioning worked
		ok, id := subject.TryGetLink(0)
		assertTrue(t, ok, "get revision")
		assertEqualInt(t, id, 9)

		rdr := subject.Freeze()
		restored := NewVersionedLink()
		err := restored.Defrost(rdr)
		assertNoError(t, err)

		// Check the restored version returns the same data
		ok, id = restored.TryGetLink(0)
		assertTrue(t, ok, "get restored revision")
		assertEqualInt(t, id, 9)
	})

	t.Run("Page links are thread safe", func(t *testing.T) {
		wait := &sync.WaitGroup{}
		subject := NewVersionedLink()

		wait.Add(1); go hammerLink(wait, subject)
		wait.Add(1); go hammerLink(wait, subject)
		wait.Add(1); go hammerLink(wait, subject)
		// each link should get 150 hits

		wait.Wait()
		assertEqualByte(t, subject.A.Version.value, 42)
		assertEqualByte(t, subject.B.Version.value, 43)
	})
}

//<editor-fold desc="Helper functions">
func hammerLink(wait *sync.WaitGroup, subject *VersionedLink) {
	for i := 0; i < 100; i++ {
		subject.WriteNewLink(i)
	}
	wait.Done()
}

func assertEqualInt(t *testing.T, actual int, expected int) {
	if actual != expected{
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}
//</editor-fold>
