# Chunked FS
# By: Zachary Cava
#
# Allows for large files to be processed by presenting the file
# as a series of chunks to process. Additionally provides a file
# tokenizer that allows files to be processed token by token.
# Assuming the file can be split into fair sized parts with tokens
# this gives the ability to process files too large to read into memory.

fs = require('fs')

# Read a file in a series of chunks rather than all at once
# This method guarantees that the given process callback will
# only be called when the byte threshold is at least reached
# @param file             [String]   the file path to the file to process
# @param processCallback  [Function] the function to call with each chunk
#                                    Any data returned from processCallback will be
#                                      appended to the beginning of the next data
#                                      passed to the callback.
#                                    If false is returned it is assumed an error occurred
# @param completeCallback [Function] the function to call when EOF is reached
#                                    or called with an error if one occurs
# @param options                    [Object]  a set of options for performance tuning
#        options.readBufferSize     [Integer] the size of the buffer to read with, defaults to 2048
#        options.chunkSizeThreshold [Integer] the byte size to trigger a process callback, defaults to 10000
#        options.byteEncoding       [String]  the encoding type, defaults to 'utf8'
chunk = (file, processCallback, completeCallback, options = {}) ->
    # Static values
    READ_BUFFER_SIZE = options.readBufferSize ? 2048
    CHUNK_SIZE_THRESHOLD = options.chunkSizeThreshold ? 10000
    ENCODING_TYPE = 'utf8'
    FILE_SIZE = 0

    # current state
    fd = null
    currentReadData = ""
    totalRead = 0

    fs.open(file, 'r', (err, openedFile) ->
        if err
            completeCallback(err)
            return

        fd = openedFile
        fstat = fs.fstatSync(fd)
        FILE_SIZE = fstat["size"]
        # start the reading process
        readChunk()
    )

    # Reads a chunk of the fiFILE_SIZEle
    readChunk = ->
        fs.read(fd, new Buffer(READ_BUFFER_SIZE), 0, READ_BUFFER_SIZE, totalRead, chunkRead)

    # A chunk of the file has been read, process it
    chunkRead = (err, bytesRead, buffer) ->
        if err
            completeCallback(err)
            return

        # count number of bytes read
        totalRead += bytesRead

        # convert buffer into correct string type
        currentReadData += buffer.toString(ENCODING_TYPE, 0, bytesRead)

        # wipe buffer reference
        buffer = null

        # if string size is above a threshold call process method
        if Buffer.byteLength(currentReadData, ENCODING_TYPE) > CHUNK_SIZE_THRESHOLD or totalRead >= FILE_SIZE
            currentReadData = processCallback(currentReadData, totalRead >= FILE_SIZE)

            if currentReadData is false
                completeCallback("False returned from process callback, assuming error occurred")
                return

            # set read to empty if method didnt return anything
            currentReadData ?= ""

        if totalRead < FILE_SIZE
            # Not yet at the end of file, read a chunk
            readChunk()
        else
            # Reached the end of file
            fs.close(fd, completeCallback)

        return

    # void return
    return

# Take a file and process it in tokens that are split by the given delimiter
# @param file             [String]   the file path of the file to process
# @param delimiter        [String]   the string that tokens will be defined by
# @param tokenCallback    [Function] the function to call with each token found in the file
# @param completeCallback [Function] the function to call when reading the file is complete
#                                      or in the case of an error, this will be called with the error
tokenize = (file, delimiter, tokenCallback, completeCallback) ->
    # Process a chunk of data into correct tokens
    processChunk = (currentBuffer, endOfFile) ->
        tokens = currentBuffer.split(delimiter)

        for token, i in tokens
            # process all but the last token, unless its the end of file
            if i isnt tokens.length - 1 or endOfFile
                tokenCallback(token)

        # Return the last data because we assume it is not complete, but
        # we assume that the next chunk will complete it
        # in the event that this is the end of the file the return will
        # be ignored anyways
        return tokens[tokens.length - 1]

    # Chunk the given file and only process tokens
    chunk(file, processChunk, completeCallback)

    # void return
    return

module.exports = {
    chunkFile : chunk
    tokenizeFile : tokenize
}