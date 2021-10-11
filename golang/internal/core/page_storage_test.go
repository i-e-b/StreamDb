package core_test

import (
	"bytes"
	"fmt"
	"golang/internal/core"
	"golang/internal/support"
	"io"
	"testing"
)

func TestCreation(t *testing.T) {
	data := core.NewMemoryRws()
	subject, err := core.NewPageStorage(data, noOpSync)
	assertNoError(t, err)

	err = subject.InitialiseDb()
	assertNoError(t, err)
}

func TestBasicUsage(t *testing.T) {
	store := core.NewMemoryRws()
	subject, err := core.NewPageStorage(store, noOpSync)
	assertNoError(t, err)

	err = subject.InitialiseDb()
	assertNoError(t, err)

	// Write a stream, bind to a doc and a path
	inputRdr := bytes.NewReader([]byte(inputData))
	pageId, err := subject.WriteStream(inputRdr)
	assertNoError(t, err)
	assertValidPage(t, pageId)

	newDoc, err := support.NewRandomId()
	assertNoError(t, err)
	expiredPageId, err := subject.BindIndex(newDoc, pageId)
	assertNoError(t, err)
	assertInvalidPage(t, expiredPageId)

	previousDocId, err := subject.BindPath("/my/path/to/poem", newDoc)
	assertNoError(t, err)
	assertNil(t, previousDocId)

	// Make sure we can see the path and ids
	headPageId, err := subject.GetDocumentHead(newDoc)
	assertNoError(t, err)
	assertPagesEqual(t, headPageId, pageId)

	docId, err := subject.GetDocumentIdByPath("/my/path/to/poem")
	assertNoError(t, err)
	assertSameId(t, docId, newDoc)

	paths, err := subject.GetPathsForDocument(docId)
	assertNoError(t, err)
	assertContains(t, paths, "/my/path/to/poem")

	// Now see if we can recover the stream
	pageStream := subject.GetStream(headPageId)
	data, err := io.ReadAll(pageStream)
	assertNoError(t,err)

	assertSameString(t, string(data), inputData)
}

func TestPageCycling(t *testing.T) {
	data := core.NewMemoryRws()
	subject, err := core.NewPageStorage(data, noOpSync)
	assertNoError(t, err)

	err = subject.InitialiseDb()
	assertNoError(t, err)

	fmt.Printf("Storage after init is %v bytes\r\n", data.Len())
	written := 0

	// read and delete a document several times
	for i := 0; i < 100; i++ {
		inputRdr := bytes.NewReader([]byte(inputData))
		pageId, err := subject.WriteStream(inputRdr)
		assertNoError(t, err)
		assertValidPage(t, pageId)

		written += len(inputData)

		c, err := subject.ReleaseChain(pageId)
		assertNoError(t, err)
		assertNonZero(t,c)
	}

	fmt.Printf("After writing and deleting %v bytes, storage is %v bytes\r\n", written, data.Len())
	if int(data.Len()) >= written {
		t.Errorf("Looks like pages are not being recycled!")
	}
}

func TestReusingLargePageChains(t *testing.T) {
	data := core.NewMemoryRws()
	subject, err := core.NewPageStorage(data, noOpSync)
	assertNoError(t, err)

	err = subject.InitialiseDb()
	assertNoError(t, err)

	sampleData := make([]byte, 32767)
	for i := 0; i < len(sampleData); i++ {
		sampleData[i] = byte(i)
	}

	var toRelease []int
	fmt.Printf("Storage after init is %v bytes\r\n", data.Len())

	// read and delete a document several times,
	// releasing chains slightly out-of-order
	for i := 0; i < 10; i++ {
		inputRdr := bytes.NewReader(sampleData)
		pageId, err := subject.WriteStream(inputRdr)
		assertNoError(t, err)
		assertValidPage(t, pageId)

		toRelease = append(toRelease, pageId)

		if len(toRelease) > 2 {
			relPage := toRelease[0]
			toRelease = toRelease[1:]
			c, err := subject.ReleaseChain(relPage)
			assertNoError(t, err)
			assertNonZero(t, c)
		}
	}
	fmt.Printf("Storage after cycling is %v bytes\r\n", data.Len())
}

func TestFreeingLargeNumberOfPages(t *testing.T) {
	data := core.NewMemoryRws()
	subject, err := core.NewPageStorage(data, noOpSync)
	assertNoError(t, err)

	err = subject.InitialiseDb()
	assertNoError(t, err)

	var toRelease []int

	// write a 1-page document many times
	for i := 0; i < 3000; i++ { // a single free-page can hold 1020 page ids
		inputRdr := bytes.NewReader([]byte(inputData))
		pageId, err := subject.WriteStream(inputRdr)
		assertNoError(t, err)
		assertValidPage(t, pageId)
		toRelease = append(toRelease, pageId)
	}

	fmt.Printf("Releasing %v pages\r\n", len(toRelease))
	for _, pageId := range toRelease {
		i, err := subject.ReleaseChain(pageId)
		assertNoError(t, err)
		assertNonZero(t, i)
	}

	length1 := data.Len()
	fmt.Printf("Storage after cycling is %v bytes\r\n", length1)

	// try to re-use the pages
	for i := 0; i < 1020; i++ {
		inputRdr := bytes.NewReader([]byte(inputData))
		pageId, err := subject.WriteStream(inputRdr) // should use the abundant free pages
		assertNoError(t, err)
		assertValidPage(t, pageId)
	}


	for i := 0; i < 10; i++ {
		inputRdr := bytes.NewReader([]byte(inputData))
		pageId, err := subject.WriteStream(inputRdr) // should use the abundant free pages
		assertNoError(t, err)
		assertValidPage(t, pageId)
	}

	length2 := data.Len()
	fmt.Printf("Storage after re-writing data is %v bytes\r\n", length2)

	if length2 >= ((length1 * 3)/2) {
		t.Errorf("Looks like the pages were not re-used correctly")
	}
}

//<editor-fold desc="Helper functions">
func assertNonZero(t *testing.T, actual int) {
	if actual == 0 {
		t.Errorf("Expected non-zero, but got zero")
	}
}

func assertSameString(t *testing.T, actual string, expected string) {
	if actual!=expected{
		t.Errorf("Expected '%v', but got '%v'", expected, actual)
	}
}

func assertContains(t *testing.T, actual []string, expected string) {
	for _, s := range actual {
		if s == expected {return}
	}
	t.Errorf("Expected '%v' but not found in %v", expected, actual)
}

func assertSameId(t *testing.T, actual *support.SerialId, expected *support.SerialId) {
	if actual.CompareTo(expected) != 0 {
		t.Errorf("Expected '%v', but got '%v'", expected, actual)
	}
}

func assertPagesEqual(t *testing.T, actual int, expected int) {
	if actual != expected {
		t.Errorf("Expected page #%v, but got #%v", expected, actual)
	}
}

func assertNil(t *testing.T, id *support.SerialId) {
	if id != nil {
		t.Errorf("Expected <nil>, but got %v", id)
	}
}

func assertInvalidPage(t *testing.T, id int) {
	if id != -1 {
		t.Errorf("Expected an invalid page (-1), but get %v", id)
	}
}

func assertValidPage(t *testing.T, id int) {
	if id < 0 {
		t.Errorf("Expected a valid page ID, but got %v", id)
	}
}

func noOpSync(_ io.ReadWriteSeeker) {}

func assertNoError(t *testing.T, err error) {
	if err != nil {
		t.Errorf("Expected no error, but got '%v'", err)
	}
}

//</editor-fold>




var inputData = "If you can keep your head when all about you   \n    Are losing theirs and blaming it on you,   \nIf you can trust yourself when all men doubt you,\n    But make allowance for their doubting too;   \nIf you can wait and not be tired by waiting,\n    Or being lied about, don’t deal in lies,\nOr being hated, don’t give way to hating,\n    And yet don’t look too good, nor talk too wise:\n\nIf you can dream—and not make dreams your master;   \n    If you can think—and not make thoughts your aim;   \nIf you can meet with Triumph and Disaster\n    And treat those two impostors just the same;   \nIf you can bear to hear the truth you’ve spoken\n    Twisted by knaves to make a trap for fools,\nOr watch the things you gave your life to, broken,\n    And stoop and build ’em up with worn-out tools:\n\nIf you can make one heap of all your winnings\n    And risk it on one turn of pitch-and-toss,\nAnd lose, and start again at your beginnings\n    And never breathe a word about your loss;\nIf you can force your heart and nerve and sinew\n    To serve your turn long after they are gone,   \nAnd so hold on when there is nothing in you\n    Except the Will which says to them: ‘Hold on!’\n\nIf you can talk with crowds and keep your virtue,   \n    Or walk with Kings—nor lose the common touch,\nIf neither foes nor loving friends can hurt you,\n    If all men count with you, but none too much;\nIf you can fill the unforgiving minute\n    With sixty seconds’ worth of distance run,   \nYours is the Earth and everything that’s in it,   \n    And—which is more—you’ll be a Man, my son!"