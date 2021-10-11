package structure

import (
	"bytes"
	"io"
	"testing"
)

func TestBasicPage(t *testing.T) {
	t.Run("Creating, freezing and defrosting", func(t *testing.T) {
		input := []byte("Buddhasaurus has prehistoric chill")

		original := NewBasicPage(5)

		err := original.Write(bytes.NewReader(input), 0, len(input))
		assertNoError(t, err)

		assertFalse(t, original.ValidateCrc(), "crc 1")
		original.UpdateCrc()
		assertTrue(t, original.ValidateCrc(), "crc 2")
		original.SetPrevPageId(4)
		assertFalse(t, original.ValidateCrc(), "crc 3")
		original.UpdateCrc()
		assertTrue(t, original.ValidateCrc(), "crc 4")

		rdr := original.Freeze()
		original = nil
		restored := NewBasicPage(5)
		err = restored.Defrost(rdr)
		assertNoError(t, err)

		assertTrue(t, restored.ValidateCrc(), "crc 5")
		assertEqualInt32(t, restored.GetPrevPageId(), 4)

		buf, err := io.ReadAll(restored.BodyReader())
		assertNoError(t, err)
		assertStringEqual(t, string(buf), string(input))
	})
}

//<editor-fold desc="Helper functions">
func assertStringEqual(t *testing.T, actual string, expected string) {
	if actual != expected {
		t.Errorf("Expected '%v', but got '%v'", expected, actual)
	}
}

func assertEqualInt32(t *testing.T, actual int32, expected int32) {
	if actual != expected {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func assertFalse(t *testing.T, value bool, msg string) {
	if value {
		t.Errorf("Expected false, but got true; %v", msg)
	}
}
func assertTrue(t *testing.T, value bool, msg string) {
	if !value {
		t.Errorf("Expected true, but got false; %v", msg)
	}
}

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Expected no error, but got '%v'", err)
	}
}

//</editor-fold>