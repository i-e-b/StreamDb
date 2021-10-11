package support

import (
	"bytes"
	"testing"
)

func TestBitwiseStreamWrapper(t *testing.T) {
	t.Run("Can write data in", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte{})
		subject := BitwiseStreamWrapper(buf, 0)

		err := subject.WriteBit(0); assertNoError(t, err) // 128
		err  = subject.WriteBit(0); assertNoError(t, err) // 64
		err  = subject.WriteBit(1); assertNoError(t, err) // 32
		err  = subject.WriteBit(0); assertNoError(t, err) // 16
		err  = subject.WriteBit(1); assertNoError(t, err) // 8
		err  = subject.WriteBit(1); assertNoError(t, err) // 4
		err  = subject.WriteBit(0); assertNoError(t, err) // 2
		err  = subject.WriteBit(1); assertNoError(t, err) // 1

		// 1+4+8+32 = 45

		assertEqualByte(t, buf.Bytes()[0], 45)
	})
	t.Run("Can write and flush partial bytes", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte{})
		subject := BitwiseStreamWrapper(buf, 0)

		for i := 0; i < 10; i++ {
			_ = subject.WriteBit(i%2)
		}
		err := subject.Flush()
		assertNoError(t, err)

		result := buf.Bytes()
		assertLength(t, result, 2)
		assertEqualByte(t, result[0], 85)
		assertEqualByte(t, result[1], 64)
	})

	t.Run("Can read bytes with run-out", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte{85,85})
		subject := BitwiseStreamWrapper(buf, 5)

		assertFalse(t, subject.IsEmpty(), "first empty check") // should have 'real' data

		for i := 0; i < 16; i++ {
			b,err := subject.ReadBit()
			assertNoError(t, err)
			assertEqualByte(t, byte(b), i%2)
		}

		subject.TryReadBit()
		assertTrue(t, subject.IsEmpty(), "second empty check") // should be out of 'real' data

		for i := 0; i < 4; i++ {
			b,ok := subject.TryReadBit()
			assertTrue(t, ok, "run out bits")
			assertEqualByte(t, byte(b), 0) // run out bits are all zero
		}

		// Should be out of run-out bits now
		_,ok := subject.TryReadBit()
		assertFalse(t, ok, "out of run out bits")

		assertTrue(t, subject.IsEmpty(), "final empty") // should still be out of 'real' data
	})

	t.Run("tight encoding test", func(t *testing.T) {
		buf := bytes.NewBuffer([]byte{})
		encoder := BitwiseStreamWrapper(buf, 0)

		err := encoder.EncodeUint(50); assertNoError(t, err)
		err  = encoder.EncodeUint(500); assertNoError(t, err)
		err  = encoder.EncodeUint(5000); assertNoError(t, err)
		err  = encoder.EncodeUint(50000); assertNoError(t, err)

		result := bytes.NewBuffer(buf.Bytes())
		decoder := BitwiseStreamWrapper(result, 0)

		val, ok := decoder.TryDecodeUint()
		assertTrue(t,ok, "1")
		assertEqualInt(t, int(val), 50)

		val, ok = decoder.TryDecodeUint()
		assertTrue(t,ok, "2")
		assertEqualInt(t, int(val), 500)

		val, ok = decoder.TryDecodeUint()
		assertTrue(t,ok, "3")
		assertEqualInt(t, int(val), 5000)

		val, ok = decoder.TryDecodeUint()
		assertTrue(t,ok, "4")
		assertEqualInt(t, int(val), 50000)
	})
}

//<editor-fold desc="Helper functions">
func assertFalse(t *testing.T, ok bool, msg string) {
	if ok {
		t.Errorf("Expected false, got true %v", msg)
	}
}

func assertTrue(t *testing.T, ok bool, msg string) {
	if !ok {
		t.Errorf("Expected true, got false %v", msg)
	}
}

func assertLength(t *testing.T, result []byte, length int) {
	if len(result) != length{
		t.Errorf("Expected %v elements, but got %v", length, len(result))
	}
}

func assertEqualByte(t *testing.T, actual byte, expected int) {
	if int(actual) != expected {
		t.Errorf("Expected %v, but got %v", expected, actual)
	}
}

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Expected no error, but got %v", err)
	}
}

//</editor-fold>