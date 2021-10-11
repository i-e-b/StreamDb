package support

import (
	"fmt"
	"testing"
)

func BenchmarkComputeCRC32(b *testing.B) {
	bytes := []byte{1, 2, 3, 5, 4, 6, 7, 8, 9, 0}

	for i := 0; i < b.N; i++ {
		_ = ComputeCRC32(bytes)
	}
}

func TestComputeCRC32(t *testing.T) {
	t.Run("crc check gives different results for different data", func(t *testing.T) {
		a := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
		b := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 0}
		c := []byte{1, 2, 3, 5, 4, 6, 7, 8, 9, 0}

		v1 := ComputeCRC32(a)
		v2 := ComputeCRC32(b)
		v3 := ComputeCRC32(c)

		fmt.Println(v1, v2, v3)

		if v1 == v2 || v1 == v3 || v2 == v3 {
			t.Errorf("Expected all results to be different. Got: %v, %v, %v", v1, v2, v3)
		}
	})
}
