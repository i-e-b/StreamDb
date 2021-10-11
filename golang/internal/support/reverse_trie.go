package support

import (
	"golang/internal/comparable"
	"bytes"
	"errors"
	"io"
	"sync"
)

// ReverseTrie is a serialisable search trie which stores
// its links from end-to-start. The
type ReverseTrie struct {
	lock *sync.RWMutex

	// store is the core list used for storage, and produces indexes. This is the only data that is serialised.
	store []rtNode

	// fwdCache is (Parent Index -> Char Value -> Child Index); This is the 'forward pointing cache' we use during construction and querying
	fwdCache map[int]charToIndex

	// valueCache is Node-to-Path mapping, for reverse look-ups. Values are entries in the `_store` list, at the end of the path.
	valueCache map[SerialComparable]nodeSet

	// valueCtor is a factory function for deserialising
	valueCtor func() SerialComparable
}

// NewReverseTrie sets up a new trie. Types stored must all
// be the kind returned by the constructor, otherwise Defrost won't work.
func NewReverseTrie(constructor func() SerialComparable) *ReverseTrie{
	trie := &ReverseTrie{
		lock:       &sync.RWMutex{},
		store:      []rtNode{},
		fwdCache:   map[int]charToIndex{},
		valueCache: map[SerialComparable]nodeSet{},
		valueCtor:  constructor,
	}

	rtAddNode(rtRootValue, rtRootParent, trie)

	return trie
}

// SerialComparable is the requirement for a value to be stored in the trie
type SerialComparable interface {
	StreamSerialisable
	comparable.Comparable
}

// Add puts a path/value pair to the trie.
// Value should not be nil. If an existing value was present, it is returned.
func (trie *ReverseTrie)Add(path string, value SerialComparable) (SerialComparable, error){
	trie.lock.Lock()
	defer trie.lock.Unlock()

	if value == nil {return nil, errors.New("value must not be nil")}
	if path == "" {return nil, errors.New("path must not be empty")}

	q := []rune(path)
	currentNode := 0

	for len(q) > 0{
		c := q[0]; q = q[1:] // de-queue

		// Find the link from current to next, if it exists already
		next := nextNode(trie, currentNode, c)
		if next > 0 {
			currentNode = next
			continue
		}

		// Not found. Add a new node linked backward
		currentNode = linkNewNode(trie, currentNode, c)
	}

	if currentNode >= len(trie.store) {panic("internal logic error in ReverseTrie.Add")}
	old := trie.store[currentNode]

	trie.store[currentNode].Data = value
	addToValueCache(trie, currentNode, value)

	return old.Data, nil
}

// Get reads the value stored on the given path. If no data is stored, nil is returned.
func (trie *ReverseTrie)Get(path string) (value SerialComparable, found bool, err error){
	trie.lock.RLock()
	defer trie.lock.RUnlock()

	if path == "" {return nil, false, errors.New("path must not be empty")}

	nodeIndex, found := tryFindNodeIndex(trie, path)
	if !found {return nil, false, nil}

	if nodeIndex >= len(trie.store) {return nil, false, errors.New("internal logic error in ReverseTrie.Get")}
	value = trie.store[nodeIndex].Data
	return value, true, nil
}

// Search returns all known paths that start with the given prefix and contain a value.
// If no keys match the prefix, an empty list is returned. Empty prefix is ignored giving an empty list.
func (trie *ReverseTrie)Search(prefix string) []string {
	trie.lock.RLock()
	defer trie.lock.RUnlock()

	var accum []string

	if prefix == "" {return accum}

	idx, found := tryFindNodeIndex(trie, prefix)
	if !found {return accum}

	allKeys := keys(trie.fwdCache[idx])

	for _, nextChar := range allKeys {
		child := trie.fwdCache[idx][nextChar]
		subset := recursiveSearch(trie, child)
		accum = append(accum, subset...)
	}
	return accum
}

// Delete the value at a path if it exists. If the path doesn't exist or has no value, this command is ignored.
func (trie *ReverseTrie)Delete(exactPath string) (valueRemoved bool) {
	trie.lock.Lock()
	defer trie.lock.Unlock()

	if exactPath == "" {return false}

	index, found := tryFindNodeIndex(trie, exactPath)
	if !found {return false}
	if index >= len(trie.store) {panic("internal logic error in ReverseTrie.Delete()")}

	old := trie.store[index].Data
	trie.store[index].Data = nil

	if old != nil {
		_, found = trie.valueCache[old]
		if found {
			delete(trie.valueCache, old)
		}
	}
	return true
}

// GetPathsForEntry lists all paths currently bound to the given value
func (trie *ReverseTrie)GetPathsForEntry(value SerialComparable) (allPaths []string) {
	if value == nil {return []string{}}
	set, found := trie.valueCache[value]
	if !found || set == nil {return []string{}}

	for _, nodeId := range set {
		allPaths = append(allPaths, traceNodePath(trie, nodeId))
	}
	return allPaths
}

// Freeze converts to a byte stream.
// Note that we only store the reverse list, the forward
// cache is rebuilt on Defrost.
func (trie *ReverseTrie)Freeze() LengthReader{
	buf := &bytes.Buffer{}
	dest := BitwiseStreamWrapper(buf, 1)

	buf.Len()

	p(dest.EncodeUint(uint32(len(trie.store)+1)))

	for _, node := range trie.store {
		// don't store root
		if node.SelfIndex == 0 {continue}

		p(dest.EncodeUint(uint32(node.Parent)))
		p(dest.EncodeUint(uint32(node.Value)))

		if node.Data == nil {
			p(dest.EncodeUint(0))
		} else {
			raw := node.Data.Freeze()
			length := raw.Len()

			p(dest.EncodeUint(uint32(length)))
			p(dest.Flush())
			n, err := buf.ReadFrom(raw)
			p(err)
			if int(n) != length {
				panic("Nonsense length")
			}
		}
	}

	// Write some zeros to pad the end of the stream
	p(dest.EncodeUint(0)) // parent
	p(dest.EncodeUint(0)) // value
	p(dest.EncodeUint(0)) // data length
	p(dest.Flush())

	return buf
}


// Defrost populates data from a byte stream
func (trie *ReverseTrie)Defrost(reader io.Reader) error {
	all, err := io.ReadAll(reader)
	if err != nil {return err}
	buf := bytes.NewBuffer(all)
	src := BitwiseStreamWrapper(buf,64)

	// Ensure we're in a start condition
	trie.store = []rtNode{}
	trie.fwdCache = map[int]charToIndex{}
	rtAddNode(rtRootValue, rtRootParent, trie)

	expectedLength, ok := src.TryDecodeUint()
	if !ok {
		return errors.New("input stream is invalid")
	}
	if expectedLength < 1 {
		return errors.New("prefix length is invalid")
	}
	expectedLength--

	for i := 0; i < int(expectedLength); i++ {
		uParent, ok := src.TryDecodeUint()
		if !ok {break}
		parent := int(uParent)
		uValue, ok := src.TryDecodeUint()
		if !ok {return errors.New("invalid structure: entry truncated at child")}
		value := rune(uValue)

		if parent == 0 && value == 0 {
			break // hit the end-of-stream marker
		}

		uDataLength, ok := src.TryDecodeUint()
		if !ok {return errors.New("invalid structure: entry truncated at data")}
		dataLength := int(uDataLength)

		if parent > len(trie.store) {
			return errors.New("invalid structure: found a parent forward of child")
		}

		newIdx := rtAddNode(value, parent, trie)
		if newIdx <= parent {return errors.New("invalid structure: found a forward pointer")}

		var cache = trie.fwdCache[parent]
		if cache == nil {
			trie.fwdCache[parent] = charToIndex{}
			cache = trie.fwdCache[parent]
		}
		cache[value] = newIdx

		if dataLength <= 0 {
			continue
		}

		// Try to read and deserialise the stored data, then store it in the trie.
		if src.IsEmpty() {return errors.New("data declared in stream run-out")}
		if buf.Len() < dataLength {return errors.New("stream was not long enough for declared data")}

		subBuf := make([]byte, dataLength)
		var n int
		n, err = buf.Read(subBuf)
		if err != nil {return err}
		if n != dataLength {return errors.New("sub-stream did not copy out completely")}

		newData := trie.valueCtor() // lack of generics!
		err = newData.Defrost(bytes.NewBuffer(subBuf))
		if err != nil {return err}

		trie.store[newIdx].Data = newData
		addToValueCache(trie, newIdx, newData)
	}
	return nil
}



func p(err error) {
	if err != nil {panic(err)}
}

// recursiveSearch walks down every child path
func recursiveSearch(trie *ReverseTrie, nodeIdx int) []string {
	var accum []string

	node := trie.store[nodeIdx]
	if node.Data != nil {
		path := traceNodePath(trie, nodeIdx)
		accum = append(accum, path)
	}

	allKeys := keys(trie.fwdCache[nodeIdx])

	for _, nextChar := range allKeys {
		child := trie.fwdCache[nodeIdx][nextChar]
		subset := recursiveSearch(trie, child)
		accum = append(accum, subset...)
	}
	return accum
}

// traceNodePath traces from the node back to root, building a string
func traceNodePath(trie *ReverseTrie, nodeIdx int) string {
	stack := []rune{}
	for nodeIdx > 0 {
		if nodeIdx >= len(trie.store) {panic("internal storage error in ReverseTrie.TraceNodePath")}
		node := trie.store[nodeIdx]
		stack = append(stack, node.Value)
		nodeIdx = node.Parent
	}
	reverseInPlace(stack)
	return string(stack)
}

func reverseInPlace(list []rune) {
	end := len(list) - 1
	for i := 0; i < end; i++ {
		if i >= end {return}
		list[i], list[end] = list[end], list[i]
		end--
	}
}

func keys(index charToIndex) []rune {
	result := make([]rune, 0, len(index))
	for r := range index {
		result = append(result, r)
	}
	return result
}


func tryFindNodeIndex(trie *ReverseTrie, path string) (nodeIndex int, found bool) {
	q := []rune(path)
	currentNode := 0

	for len(q) > 0 {
		c := q[0]; q = q[1:] // dequeue
		next := nextNode(trie, currentNode, c)
		if next < 0 {return -1, false}

		currentNode = next
	}
	return currentNode, true
}

func addToValueCache(trie *ReverseTrie, newIdx int, data SerialComparable) {
	cache := trie.valueCache

	_, found := cache[data]
	if ! found {
		cache[data] = map[int]int{}
	}

	cache[data][newIdx] = newIdx
}

func nextNode(trie *ReverseTrie, currentNode int, c rune) (nextNode int) {
	var nodeLinks = trie.fwdCache[currentNode]
	if nodeLinks == nil {
		trie.fwdCache[currentNode] = charToIndex{}
		nodeLinks = trie.fwdCache[currentNode]
	}
	value, found := nodeLinks[c]
	if !found {
		return -1
	}
	return value
}

func linkNewNode(trie *ReverseTrie, currentNode int, c rune) (newNode int) {
	idx := rtAddNode(c, currentNode, trie)

	nodeLinks := trie.fwdCache[currentNode]
	if nodeLinks == nil {panic("internal storage error in ReverseTrie.LinkNewNode()")}

	nodeLinks[c] = idx
	return idx
}

//<editor-fold desc="Nodes and types">
type nodeSet map[int]int
type charToIndex map[rune]int

// rtNode is a single point in the trie
type rtNode struct {
	Value rune
	Parent int
	SelfIndex int

	Data SerialComparable
}

// rtAddNode inserts a node into a trie, and returns the storage index
func rtAddNode(value rune, parent int, target *ReverseTrie) int{
	idx := len(target.store)
	node:= rtNode{
		Value: value,
		Parent: parent,
		SelfIndex: idx,
	}

	target.store = append(target.store, node)
	return idx
}

func (node *rtNode)CompareTo(other interface{}) int{
	otherNode, ok := other.(*rtNode)
	if !ok {
		oc2, ok2 := other.(rtNode)
		if !ok2 {return 0}
		otherNode = &oc2
	}

	return node.Data.CompareTo(otherNode.Data)
}

const (
	rtRootValue = rune(0) // all strings point back to a single common root, at index zero.
	rtRootParent = -1
)

//</editor-fold>