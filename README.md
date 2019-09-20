# StreamDb

A minimalist database that uses only Streams as storage, and can survive power loss and data corruption

## The plan:

* A generalised page storage system, using fixed size pages
* Every page is monotonically versioned
* Every page contains enough meta data to reconstruct a partial database
* Pages are garbage collected once enough versions behind -- perhaps only by explicit user request
* Data is referred to with a GUID identifier
* Data is stored as binaries objects
* A special path object is maintained to link string paths to object GUIDs

## Goals:

1. Be reliable
2. No 3rd party dependencies, run in most basic C# environment possible
3. Be reasonably fast
4. Don't waste too much disk space
5. Small code

## Non Goals:

* Any kind of advanced querying, searching, etc.
* Be the fastest / smallest / anything-est.



## Notes:

For testing, a stream wrapper that stops writing at a random point (but still acts like it's writing). Run that loads of times to fuzz.
