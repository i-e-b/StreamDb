# StreamDb

A minimalist database that uses only Streams as storage, and can survive power loss and data corruption.
This is working, but don't use it for anything important without significant testing.

There is also a small WinForms app to explore StreamDB files created by other software.

## The plan:

* A generalised page storage system, using fixed size pages
* Every page is monotonically versioned
* Pages are garbage collected once enough versions behind
* Data is referred to with a GUID identifier
* Data is stored as binary objects
* A special path object is maintained to link string paths to object GUIDs

## Goals:

1. Be reliable (specifically, don't corrupt the database if the host process crashes)
2. No 3rd party dependencies, run in most basic C# environment possible (i.e. .Net Standard)
3. Be reasonably fast
4. Don't waste too much disk space (i.e. re-use deleted pages and very old versions)
	4.a. It's ok to waste some space if it helps reliability
5. Small code

## Non Goals:

* Any kind of advanced querying, searching, etc.
* Be the fastest / smallest / anything-est.
* Structure (de)serialisation. Users should provide byte streams, and will get byte streams back.

## To-do / progress

* [ ] Remove the special cases for index and free-list, make them a regular stream like the trie.
* [ ] Remove the version request, replace with a 'prev' method.
* [ ] Replace `lock` with non-recursive mutex calls (for portability)
* [ ] Finish port to Golang