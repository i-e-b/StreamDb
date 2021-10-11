package support

import (
	"golang/internal/comparable"
	"testing"
)

func BenchmarkSerialId(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = NewRandomId()
	}
}

func TestSerialId(t *testing.T) {
	t.Run("can create lots of unique ids", func(t *testing.T) {
		prev := NewZeroId()
		for i := 0; i < 100; i++ {
			a, err := NewRandomId()
			assertNoError(t,err)
			cmp := prev.CompareTo(a)
			assertNotZero(t,cmp)
		}
	})

	t.Run("can store and restore an id", func(t *testing.T) {
		original, err := NewRandomId(); assertNoError(t, err)
		copyId := NewZeroId()
		assertNotZero(t, original.CompareTo(copyId))

		rdr := original.Freeze()
		err = copyId.Defrost(rdr); assertNoError(t, err)

		assertTrue(t, comparable.Is(original).EqualTo(copyId), "copy is not equal")
	})
}


//<editor-fold desc="Helper functions">
func assertNotZero(t *testing.T, cmp int) {
	if cmp == 0 {
		t.Errorf("Expected non-zero, but got zero")
	}
}
//</editor-fold>