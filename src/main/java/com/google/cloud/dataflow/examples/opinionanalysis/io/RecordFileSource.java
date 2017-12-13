/*******************************************************************************
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package com.google.cloud.dataflow.examples.opinionanalysis.io;

import static com.google.common.base.Preconditions.checkState;


import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

/**
 * A {@link FileBasedSource} which can decode records delimited by characters.
 * Generalized from on org.apache.beam.sdk.io.TextSource.
 *
 * <p>This source splits the data into records using characters passed as the delimiter. 
 * This source is not strict and supports decoding the last record
 * even if it is not delimited. Finally, no records are decoded if the stream is empty.
 *
 * <p>This source supports reading from any arbitrary byte position within the stream. If the
 * starting position is not {@code 0}, then bytes are skipped until the first delimiter is found
 * representing the beginning of the first record to be decoded.
 */

public class RecordFileSource<T> extends FileBasedSource<T> {
	
  /** The Coder to use to decode each record. */
  private final Coder<T> coder;
  /** The separator to use to separate the records in a single file */
  private final byte separator;

  public static final byte DEFAULT_RECORD_SEPARATOR = '\036'; // use ASCII Record Separator RS octal number 036, decimal 30, hex 1E
  public static final byte CR_RECORD_SEPARATOR = '\015'; // CR: octal number 015, decimal 13, hex 0D
	
	
  public RecordFileSource(ValueProvider<String> fileSpec, Coder<T> coder, byte separator) {
    super(fileSpec, 1L);
    this.coder = coder;
    this.separator = separator;
  }

  private RecordFileSource(MatchResult.Metadata metadata, long start, long end, Coder<T> coder, byte separator) {
    super(metadata, 1L, start, end);
    this.coder = coder;
    this.separator = separator;
  }

  @Override
  protected FileBasedSource<T> createForSubrangeOfFile(
      MatchResult.Metadata metadata,
      long start,
      long end) {
    return new RecordFileSource<>(metadata, start, end, coder, separator);
  }

  @Override
  protected FileBasedReader<T> createSingleFileReader(PipelineOptions options) {
    return new RecordFileReader<>(this);
  }

  @Override
  public Coder<T> getDefaultOutputCoder() {
    return coder;
  }

  /**
   * A {@link RecordFileReader}
   * which can decode records delimited by separator character passed in the constructor.
   *
   * See {@link RecordFileSource} for further details.
   */
  
  public static class RecordFileReader<T> extends FileBasedReader<T> {
	private final Coder<T> coder;
	private final byte separator;
    private static final int READ_BUFFER_SIZE = 8192;
    private final ByteBuffer readBuffer = ByteBuffer.allocate(READ_BUFFER_SIZE);
    private ByteString buffer;
    private int startOfSeparatorInBuffer;
    private int endOfSeparatorInBuffer;
    private long startOfRecord;
    private volatile long startOfNextRecord;
    private volatile boolean eof;
    private volatile boolean elementIsPresent;
    private T currentValue;
    private ReadableByteChannel inChannel;

    private RecordFileReader(RecordFileSource<T> source) {
      super(source);
      buffer = ByteString.EMPTY;
      coder = source.coder;
      separator = source.separator;
    }

    @Override
    protected long getCurrentOffset() throws NoSuchElementException {
      if (!elementIsPresent) {
        throw new NoSuchElementException();
      }
      return startOfRecord;
    }

    @Override
    public long getSplitPointsRemaining() {
      if (isStarted() && startOfNextRecord >= getCurrentSource().getEndOffset()) {
        return isDone() ? 0 : 1;
      }
      return super.getSplitPointsRemaining();
    }

    @Override
    public T getCurrent() throws NoSuchElementException {
      if (!elementIsPresent) {
        throw new NoSuchElementException();
      }
      return currentValue;
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      this.inChannel = channel;
      // If the first offset is greater than zero, we need to skip bytes until we see our
      // first separator.
      if (getCurrentSource().getStartOffset() > 0) {
        checkState(channel instanceof SeekableByteChannel,
            "%s only supports reading from a SeekableByteChannel when given a start offset"
            + " greater than 0.", RecordFileSource.class.getSimpleName());
        long requiredPosition = getCurrentSource().getStartOffset() - 1;
        ((SeekableByteChannel) channel).position(requiredPosition);
        findSeparatorBounds();
        buffer = buffer.substring(endOfSeparatorInBuffer);
        startOfNextRecord = requiredPosition + endOfSeparatorInBuffer;
        endOfSeparatorInBuffer = 0;
        startOfSeparatorInBuffer = 0;
      }
    }

    /**
     * Locates the start position and end position of the next delimiter. Will
     * consume the channel till either EOF or the delimiter bounds are found.
     *
     * <p>This fills the buffer and updates the positions as follows:
     * <pre>{@code
     * ------------------------------------------------------
     * | element bytes | delimiter bytes | unconsumed bytes |
     * ------------------------------------------------------
     * 0            start of          end of              buffer
     *              separator         separator           size
     *              in buffer         in buffer
     * }</pre>
     */
    private void findSeparatorBounds() throws IOException {
      int bytePositionInBuffer = 0;
      while (true) {
        if (!tryToEnsureNumberOfBytesInBuffer(bytePositionInBuffer + 1)) {
          startOfSeparatorInBuffer = endOfSeparatorInBuffer = bytePositionInBuffer;
          break;
        }

        byte currentByte = buffer.byteAt(bytePositionInBuffer);

        if (currentByte == this.separator) {
          startOfSeparatorInBuffer = bytePositionInBuffer;
          endOfSeparatorInBuffer = startOfSeparatorInBuffer + 1;
          break;
        }

        // Move to the next byte in buffer.
        bytePositionInBuffer += 1;
      }
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      startOfRecord = startOfNextRecord;
      findSeparatorBounds();

      // If we have reached EOF file and consumed all of the buffer then we know
      // that there are no more records.
      if (eof && buffer.size() == 0) {
        elementIsPresent = false;
        return false;
      }

      decodeCurrentElement();
      startOfNextRecord = startOfRecord + endOfSeparatorInBuffer;
      return true;
    }

    /**
     * Decodes the current element updating the buffer to only contain the unconsumed bytes.
     *
     * <p>This invalidates the currently stored {@code startOfSeparatorInBuffer} and
     * {@code endOfSeparatorInBuffer}.
     */
    private void decodeCurrentElement() throws IOException {
      ByteString dataToDecode = buffer.substring(0, startOfSeparatorInBuffer);
      // sso 7/12/2017: TODO: the 2.0 method call , without Context.OUTER, returns gibberish
      // restoring 1.9 code, and need to follow up with the SDK team
      // currentValue = coder.decode(dataToDecode.newInput()); // currentValue = dataToDecode.toStringUtf8();
      currentValue = coder.decode(dataToDecode.newInput(), Context.OUTER);
      elementIsPresent = true;
      buffer = buffer.substring(endOfSeparatorInBuffer);
    }

    /**
     * Returns false if we were unable to ensure the minimum capacity by consuming the channel.
     */
    private boolean tryToEnsureNumberOfBytesInBuffer(int minCapacity) throws IOException {
      // While we aren't at EOF or haven't fulfilled the minimum buffer capacity,
      // attempt to read more bytes.
      while (buffer.size() <= minCapacity && !eof) {
        eof = inChannel.read(readBuffer) == -1;
        readBuffer.flip();
        buffer = buffer.concat(ByteString.copyFrom(readBuffer));
        readBuffer.clear();
      }
      // Return true if we were able to honor the minimum buffer capacity request
      return buffer.size() >= minCapacity;
    }
  }
}
