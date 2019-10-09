# StreamDb

A minimalist database that uses only Streams as storage, and can survive power loss and data corruption

## The plan:

* A generalised page storage system, using fixed size pages
* Every page is monotonically versioned
* Every page contains enough meta data to reconstruct a partial database
* Pages are garbage collected once enough versions behind
* Data is referred to with a GUID identifier
* Data is stored as binary objects
* A special path object is maintained to link string paths to object GUIDs

## Goals:

1. Be reliable (specifically, don't corrupt the database if the host process crashes)
2. No 3rd party dependencies, run in most basic C# environment possible
3. Be reasonably fast
4. Don't waste too much disk space (i.e. re-use deleted pages and very old versions)
5. Small code

## Non Goals:

* Any kind of advanced querying, searching, etc.
* Be the fastest / smallest / anything-est.
* Structure (de)serialisation. Users should provide byte streams, and will get byte streams back.

## To-do:

* [ ] Try replacing CRC with some kind of FEC code <-- going to experiment here
* [ ] Improve page structure to support more robust versioning
* [ ] Improve path index to support multiple versions
* [ ] Better free-list structure (more thread safe, better balancing)
* [x] Thread safety tests
* [x] Complete database entry point stuff
* [x] Re-write path lookup serialisation, so that it's append only
* [x] Update path lookup pages to append data
* [x] Support partly-full pages at the page header level
* [x] Improve data transport to reduce copying and GC

## Notes:

A rework of the path index is in order. I think a paged hash table (like the current doc id index) would be better.
A rework of the page table is also probably needed.

The CRC checks are taking 50% of run time.

For testing, a stream wrapper that stops writing at a random point (but still acts like it's writing). Run that loads of times to fuzz.

Links that can change are always in pairs. We take the highest version, and check the target exists and
has a valid CRC. If not, fall back to the previous. Version numbers in links are a monotonic byte counter.

Generally, we point to the end of a list and link backwards. That way we can add to the database non-destructively.

The binary-tree index is per-data-page. Each index has a completely separate root and tree. When reading, if you can't find the ID you are
looking for, move to the next page (I don't expect a lot of queries for non-items unless the dataset is sparse. The path lookup trie should also minimise total fails).
Maybe have a fully implicit tree, and don't worry about complete occupation -- just move to the next page if there are no spaces.

Deleting a document is marking it with a flag, is this a change in version? Delete flags should propagate back through chains.

We have bi-directional links for two reasons: Optimising reading multi-page documents; Data recovery (need to lose at least 2 links before a page is lost)
BinaryTree entry page index should have link to both start and end of page chain.
