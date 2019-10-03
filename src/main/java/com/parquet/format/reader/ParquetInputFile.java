package com.parquet.format.reader;

import com.parquet.format.reader.ParquetSeekable;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.InputStream;

public class ParquetInputFile implements InputFile {
  private InputStream inputStream;

  ParquetInputFile(InputStream inputStream) {
    this.inputStream  = inputStream;
  }

  public long getLength () throws IOException {
    return this.inputStream.available();
  }

  public SeekableInputStream newStream () {
    return new ParquetSeekable(this.inputStream);
  }
}
