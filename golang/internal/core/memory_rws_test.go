package core

import (
	"io"
	"testing"
)

func TestMemoryStream(t *testing.T) {
	subject := NewMemoryRws()

	source := []byte("But I must explain to you how all this mistaken idea of denouncing pleasure and praising pain was born and I will give you a complete account of the system.")
	written, err := subject.Write(source)
	if err != nil {t.Errorf("write failed: %v", err)}

	if written != len(source){
		t.Errorf("Expected to write %v, but got %v", len(source), written)
	}

	t.Run("Reading past end", func(t *testing.T) {
		pos, err := subject.Seek(0, io.SeekStart)
		if err != nil {t.Errorf("seek failed: %v", err)}
		if pos != 0 {t.Errorf("Expected position 0, got %v", pos)}

		dest := make([]byte, len(source)*2)
		read, err := subject.Read(dest)
		if err != nil {t.Errorf("read failed: %v", err)}
		if read != len(source) {
			t.Errorf("Expected to read %v, but got %v", len(source), read)
		}
	})

	t.Run("Reading small chunk at start", func(t *testing.T) {
		pos, err := subject.Seek(0, io.SeekStart)
		if err != nil {t.Errorf("seek failed: %v", err)}
		if pos != 0 {t.Errorf("Expected position 0, got %v", pos)}

		dest := []byte{0,0,0,0}
		read, err := subject.Read(dest)
		if err != nil {t.Errorf("read failed: %v", err)}
		if read != len(dest) {
			t.Errorf("Expected to read %v, but got %v", len(dest), read)
		}
		if string(dest) != "But " {
			t.Errorf("Expected %s, but got %s", "But ", dest)
		}
	})

	t.Run("Reading small chunk at middle", func(t *testing.T) {
		pos, err := subject.Seek(20, io.SeekStart)
		if err != nil {t.Errorf("seek failed: %v", err)}
		if pos != 20 {t.Errorf("Expected position 20, got %v", pos)}

		dest := []byte{0,0,0,0}
		read, err := subject.Read(dest)
		if err != nil {t.Errorf("read failed: %v", err)}
		if read != len(dest) {
			t.Errorf("Expected to read %v, but got %v", len(dest), read)
		}
		if string(dest) != "o yo" {
			t.Errorf("Expected %s, but got %s", "o yo", string(dest))
		}
	})

	t.Run("Reading small chunk at end", func(t *testing.T) {
		pos, err := subject.Seek(-4, io.SeekEnd)
		if err != nil {t.Errorf("seek failed: %v", err)}
		if pos != 152 {t.Errorf("Expected position 152, got %v", pos)}

		dest := []byte{0,0,0,0}
		read, err := subject.Read(dest)
		if err != nil {t.Errorf("read failed: %v", err)}
		if read != len(dest) {
			t.Errorf("Expected to read %v, but got %v", len(dest), read)
		}
		if string(dest) != "tem." {
			t.Errorf("Expected %s, but got %s", "tem.", string(dest))
		}
	})

	t.Run("Multiple seek", func(t *testing.T) {
		_, err := subject.Seek(5, io.SeekStart)
		if err != nil {t.Errorf("seek failed: %v", err)}
		for i := 0; i < 5; i++ {
			_, err = subject.Seek(5, io.SeekCurrent)
			if err != nil {t.Errorf("seek failed: %v", err)}
		}


		pos, err := subject.Seek(5, io.SeekCurrent)
		if err != nil {t.Errorf("seek failed: %v", err)}
		if pos != 35 {
			t.Errorf("Expected position %v, but got %v", 35, pos)
		}
	})
}
