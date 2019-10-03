package com.parquet.format.reader;

import com.parquet.format.reader.Exception.UnSupportedStreamTypeException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class ParquetFormat {

  private List<String> records;

  /**
   * This Method is used to return all the Parquet records from an InputStream
   * @return List<String> containing all the records from InputStream
   * @param inputStream must be of type FSDataInputStream or FileInputStream
   * @throws IOException
   */
  public  List<String> getParquetRecords (InputStream inputStream)
          throws IOException {

    if (!(inputStream instanceof FileInputStream)) {
      if (!(inputStream instanceof FSDataInputStream) ) {
        throw new UnSupportedStreamTypeException("InputStream expected of type "
                + FileInputStream.class.getName()
                + " or " + FSDataInputStream.class.getName() + " but found "
                + inputStream.getClass().getName());
      }
    }

    InputFile inputFile = new ParquetInputFile(inputStream);
    this.records = new ArrayList();
    ParquetFileReader reader =
            ParquetFileReader.open(inputFile);
    MessageType schema = reader.getFooter().getFileMetaData().getSchema();
    PageReadStore pages;
    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader recordReader = columnIO.getRecordReader(pages
              , new GroupRecordConverter(schema));

      for (int i = 0; i < rows; i++) {
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        this.records.add(simpleGroup.toString());
      }
    }
    reader.close();
    return this.records;
  }
}
