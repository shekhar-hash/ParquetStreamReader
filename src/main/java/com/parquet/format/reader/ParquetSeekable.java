package com.parquet.format.reader;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.parquet.io.DelegatingSeekableInputStream;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ParquetSeekable
        extends DelegatingSeekableInputStream {
  private InputStream inputStream;
  private long position;

  ParquetSeekable(InputStream inputStream) {
    super(inputStream);
    this.inputStream = inputStream;
    this.position = 0;
  }

  @Override
  public long getPos () throws IOException {
    return this.position;
  }

  @Override
  public void seek (long l) throws IOException {
    if (inputStream instanceof FSDataInputStream ) {
      ((FSDataInputStream) inputStream).seek(l);
    } else if (inputStream instanceof FileInputStream ) {
      if (l < 0) {
        throw new EOFException(
                FSExceptionMessages.NEGATIVE_SEEK);
      }
      ((FileInputStream) inputStream)
              .getChannel().position(l);
    }
    this.position = l;
  }
}
