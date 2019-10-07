package com.parquet.format.reader;


import com.parquet.format.reader.Exception.UnSupportedStreamTypeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class ParquetReadTest {

  private static final String TEMP_FILE_PATH = "src/test/testResources/test.parquet";
  private static final int NO_OF_RECORDS = 200000;

  @Before
  public void createTempFile() throws IOException {
    ParquetTestUtil.parquetRecordWriter(NO_OF_RECORDS, TEMP_FILE_PATH);
  }

  @After
  public void deleteTempFile() throws IOException {
    File file = new File(TEMP_FILE_PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void fileInputStreamTest() throws IOException {
    InputStream inputStream = new FileInputStream(new File(TEMP_FILE_PATH));
    ParquetFormat parquetFormat = new ParquetFormat();
    List<SimpleGroup> list = parquetFormat.getParquetRecords(inputStream).getList();
    Assert.assertEquals(NO_OF_RECORDS, list.size());
  }

  @Test
  public void fSDataInputStreamTest() throws IOException {
    FileSystem fileSystem = FileSystem.newInstance(new Configuration());
    InputStream inputStream = new FSDataInputStream(fileSystem.open(new Path(TEMP_FILE_PATH)));
    ParquetFormat parquetFormat = new ParquetFormat();
    List<SimpleGroup> list = parquetFormat.getParquetRecords(inputStream).getList();
    Assert.assertEquals(NO_OF_RECORDS, list.size());
  }

  @Test(expected = UnSupportedStreamTypeException.class)
  public void unSupportedFileSystem() throws IOException {
    InputStream inputStream = new InputStream() {
      @Override
      public int read () throws IOException {
        return 0;
      }
    };
    ParquetFormat parquetFormat = new ParquetFormat();
    parquetFormat.getParquetRecords(inputStream);
  }
}
