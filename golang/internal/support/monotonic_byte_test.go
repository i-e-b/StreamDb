package support

import (
	"golang/internal/comparable"
	"testing"
)

func TestMonotonicByte_Increment(t *testing.T) {
	t.Run("an incremented value is always considered greater than its source", func(t *testing.T) {
		lower := NewMonotonicByte(0)
		upper := NewMonotonicByte(1)

		for r := 0; r < 63; r++ { // drift range (0..64, we start at drift=1)
			for i := 0; i < 512; i++ { // cycling, make sure we can overflow correctly
				if comparable.Is(lower).GreaterOrEqual(upper) {
					t.Errorf("Drift = {%v}; Compared {%v} < {%v} incorrectly", r, lower.value, upper.value)
				}
				lower.Increment()
				upper.Increment()
			}
			upper.Increment()
		}
	})
	t.Run("monotonic bytes can loop", func(t *testing.T) {
		subject := NewMonotonicByte(0)
		for i := 0; i < 384; i++ {
			subject.Increment()
		}
		if int(subject.value) >= 384 {
			t.Errorf("Subject did not loop. Expected 128, got %v", subject.value)
		}
	})
}

func TestMonotonicByte_Freeze(t *testing.T) {
	t.Run("monotonic count can be serialised and restored", func(t *testing.T) {
		source := NewMonotonicByte(140)
		bytes := source.Freeze()

		dest := NewMonotonicByte(0)
		err := dest.Defrost(bytes)
		if err != nil {
			t.Errorf("Error when defrosting: %v", err)
		}
		if comparable.Is(source).NotEqual(dest){
			t.Errorf("Expected two counters to be equal, got %v and %v", source, dest)
		}
	})
}