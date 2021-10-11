package comparable


type Comparable interface {
	// CompareTo should return -1 or negative if the 'other' is greater,
	// 1 or positive if the other is less, 0 if equal or not comparable.
	CompareTo(other interface{}) int
}


// HoldsComparable uses the Is function to make comparisons fluent
type HoldsComparable struct {
	thing Comparable
}
func Is(thing Comparable) HoldsComparable {
	return HoldsComparable{thing: thing}
}
func (cmp HoldsComparable) GreaterThan(other Comparable) bool {
	val := cmp.thing.CompareTo(other)
	return val > 0
}
func (cmp HoldsComparable) GreaterOrEqual(other Comparable) bool {
	val := cmp.thing.CompareTo(other)
	return val >= 0
}
func (cmp HoldsComparable) LessThan(other Comparable) bool {
	val := cmp.thing.CompareTo(other)
	return val < 0
}
func (cmp HoldsComparable) NotEqual(other Comparable) bool {
	val := cmp.thing.CompareTo(other)
	return val != 0
}
func (cmp HoldsComparable) EqualTo(other Comparable) bool {
	val := cmp.thing.CompareTo(other)
	return val == 0
}