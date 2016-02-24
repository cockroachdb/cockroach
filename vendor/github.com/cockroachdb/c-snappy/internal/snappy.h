// Copyright 2005 and onwards Google Inc.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
// A light-weight compression algorithm.  It is designed for speed of
// compression and decompression, rather than for the utmost in space
// savings.
//
// For getting better compression ratios when you are compressing data
// with long repeated sequences or compressing data that is similar to
// other data, while still compressing fast, you might look at first
// using BMDiff and then compressing the output of BMDiff with
// Snappy.

#ifndef THIRD_PARTY_SNAPPY_SNAPPY_H__
#define THIRD_PARTY_SNAPPY_SNAPPY_H__

#include <stddef.h>
#include <string>

#include "snappy-stubs-public.h"

namespace snappy {
  class Source;
  class Sink;

  // ------------------------------------------------------------------------
  // Generic compression/decompression routines.
  // ------------------------------------------------------------------------

  // Compress the bytes read from "*source" and append to "*sink". Return the
  // number of bytes written.
  size_t Compress(Source* source, Sink* sink);

  // Find the uncompressed length of the given stream, as given by the header.
  // Note that the true length could deviate from this; the stream could e.g.
  // be truncated.
  //
  // Also note that this leaves "*source" in a state that is unsuitable for
  // further operations, such as RawUncompress(). You will need to rewind
  // or recreate the source yourself before attempting any further calls.
  bool GetUncompressedLength(Source* source, uint32* result);

  // ------------------------------------------------------------------------
  // Higher-level string based routines (should be sufficient for most users)
  // ------------------------------------------------------------------------

  // Sets "*output" to the compressed version of "input[0,input_length-1]".
  // Original contents of *output are lost.
  //
  // REQUIRES: "input[]" is not an alias of "*output".
  size_t Compress(const char* input, size_t input_length, string* output);

  // Decompresses "compressed[0,compressed_length-1]" to "*uncompressed".
  // Original contents of "*uncompressed" are lost.
  //
  // REQUIRES: "compressed[]" is not an alias of "*uncompressed".
  //
  // returns false if the message is corrupted and could not be decompressed
  bool Uncompress(const char* compressed, size_t compressed_length,
                  string* uncompressed);

  // Decompresses "compressed" to "*uncompressed".
  //
  // returns false if the message is corrupted and could not be decompressed
  bool Uncompress(Source* compressed, Sink* uncompressed);

  // This routine uncompresses as much of the "compressed" as possible
  // into sink.  It returns the number of valid bytes added to sink
  // (extra invalid bytes may have been added due to errors; the caller
  // should ignore those). The emitted data typically has length
  // GetUncompressedLength(), but may be shorter if an error is
  // encountered.
  size_t UncompressAsMuchAsPossible(Source* compressed, Sink* uncompressed);

  // ------------------------------------------------------------------------
  // Lower-level character array based routines.  May be useful for
  // efficiency reasons in certain circumstances.
  // ------------------------------------------------------------------------

  // REQUIRES: "compressed" must point to an area of memory that is at
  // least "MaxCompressedLength(input_length)" bytes in length.
  //
  // Takes the data stored in "input[0..input_length]" and stores
  // it in the array pointed to by "compressed".
  //
  // "*compressed_length" is set to the length of the compressed output.
  //
  // Example:
  //    char* output = new char[snappy::MaxCompressedLength(input_length)];
  //    size_t output_length;
  //    RawCompress(input, input_length, output, &output_length);
  //    ... Process(output, output_length) ...
  //    delete [] output;
  void RawCompress(const char* input,
                   size_t input_length,
                   char* compressed,
                   size_t* compressed_length);

  // Given data in "compressed[0..compressed_length-1]" generated by
  // calling the Snappy::Compress routine, this routine
  // stores the uncompressed data to
  //    uncompressed[0..GetUncompressedLength(compressed)-1]
  // returns false if the message is corrupted and could not be decrypted
  bool RawUncompress(const char* compressed, size_t compressed_length,
                     char* uncompressed);

  // Given data from the byte source 'compressed' generated by calling
  // the Snappy::Compress routine, this routine stores the uncompressed
  // data to
  //    uncompressed[0..GetUncompressedLength(compressed,compressed_length)-1]
  // returns false if the message is corrupted and could not be decrypted
  bool RawUncompress(Source* compressed, char* uncompressed);

  // Given data in "compressed[0..compressed_length-1]" generated by
  // calling the Snappy::Compress routine, this routine
  // stores the uncompressed data to the iovec "iov". The number of physical
  // buffers in "iov" is given by iov_cnt and their cumulative size
  // must be at least GetUncompressedLength(compressed). The individual buffers
  // in "iov" must not overlap with each other.
  //
  // returns false if the message is corrupted and could not be decrypted
  bool RawUncompressToIOVec(const char* compressed, size_t compressed_length,
                            const struct iovec* iov, size_t iov_cnt);

  // Given data from the byte source 'compressed' generated by calling
  // the Snappy::Compress routine, this routine stores the uncompressed
  // data to the iovec "iov". The number of physical
  // buffers in "iov" is given by iov_cnt and their cumulative size
  // must be at least GetUncompressedLength(compressed). The individual buffers
  // in "iov" must not overlap with each other.
  //
  // returns false if the message is corrupted and could not be decrypted
  bool RawUncompressToIOVec(Source* compressed, const struct iovec* iov,
                            size_t iov_cnt);

  // Returns the maximal size of the compressed representation of
  // input data that is "source_bytes" bytes in length;
  size_t MaxCompressedLength(size_t source_bytes);

  // REQUIRES: "compressed[]" was produced by RawCompress() or Compress()
  // Returns true and stores the length of the uncompressed data in
  // *result normally.  Returns false on parsing error.
  // This operation takes O(1) time.
  bool GetUncompressedLength(const char* compressed, size_t compressed_length,
                             size_t* result);

  // Returns true iff the contents of "compressed[]" can be uncompressed
  // successfully.  Does not return the uncompressed data.  Takes
  // time proportional to compressed_length, but is usually at least
  // a factor of four faster than actual decompression.
  bool IsValidCompressedBuffer(const char* compressed,
                               size_t compressed_length);

  // Returns true iff the contents of "compressed" can be uncompressed
  // successfully.  Does not return the uncompressed data.  Takes
  // time proportional to *compressed length, but is usually at least
  // a factor of four faster than actual decompression.
  // On success, consumes all of *compressed.  On failure, consumes an
  // unspecified prefix of *compressed.
  bool IsValidCompressed(Source* compressed);

  // The size of a compression block. Note that many parts of the compression
  // code assumes that kBlockSize <= 65536; in particular, the hash table
  // can only store 16-bit offsets, and EmitCopy() also assumes the offset
  // is 65535 bytes or less. Note also that if you change this, it will
  // affect the framing format (see framing_format.txt).
  //
  // Note that there might be older data around that is compressed with larger
  // block sizes, so the decompression code should not rely on the
  // non-existence of long backreferences.
  static const int kBlockLog = 16;
  static const size_t kBlockSize = 1 << kBlockLog;

  static const int kMaxHashTableBits = 14;
  static const size_t kMaxHashTableSize = 1 << kMaxHashTableBits;
}  // end namespace snappy

#endif  // THIRD_PARTY_SNAPPY_SNAPPY_H__
