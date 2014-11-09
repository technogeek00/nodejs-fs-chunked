FS Chunked
==========
By Zachary Cava

Provides the ability to process a given file as a series of chunks with ```chunkFile```. This functionality is not new to node, but it wraps some of the underlying management so you only have to specify a filename and a process callback. The main feature of this module is the ```tokenizeFile``` method that will process a given file as a series of tokens.

Due to the chunked nature of the processing large files can be processed that would otherwise be impossible to read completely into memory. This module is part of a larger data processing project that will be released in time.
