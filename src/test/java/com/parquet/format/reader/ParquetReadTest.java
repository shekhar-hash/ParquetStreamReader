package com.parquet.format.reader;


import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ParquetReadTest {

  private static final String TEMP_FILE_PATH = "src/test/testResources/test.parquet";
  private static final int NO_OF_RECORDS = 20;

  @Before
  public void createTempFile() throws IOException {
    ParquetTestUtil.parquetRecordWriter(NO_OF_RECORDS, TEMP_FILE_PATH);
  }

  @Test
  public void fileInputStreamTest() throws IOException {
    InputStream inputStream = new FileInputStream(new File(TEMP_FILE_PATH));
    ParquetFormat parquetFormat = new ParquetFormat();
    List<String> list = parquetFormat.getParquetRecords(inputStream);
    Assert.assertEquals(NO_OF_RECORDS, list.size());
  }

  @Test
  public void fSDataInputStreamTest() throws IOException {
    FileSystem fileSystem = FileSystem.newInstance(new Configuration());
    InputStream inputStream = new FSDataInputStream(fileSystem.open(new Path(TEMP_FILE_PATH)));
    ParquetFormat parquetFormat = new ParquetFormat();
    List<String> list = parquetFormat.getParquetRecords(inputStream);
    Assert.assertEquals(NO_OF_RECORDS, list.size());
  }

  @Test(expected = IOException.class)
  public void unSupportedFileSystem() throws IOException {
    InputStream inputStream = new InputStream() {
      @Override
      public int read () throws IOException {
        return 0;
      }
    };
    ParquetFormat parquetFormat = new ParquetFormat();
    List<String> list = parquetFormat.getParquetRecords(inputStream);
  }
}
