package com.parquet.format.reader;

import com.parquet.format.reader.Exception.UnSupportedStreamTypeException;
import org.apache.avro.reflect.AvroSchema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.log4j.BasicConfigurator;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ParquetFormat {

  private static final Logger log = LoggerFactory.getLogger(ParquetFormat.class);

  /**
   * This Method is used to return all the Parquet records from given {@link InputStream}
   * @return {@link List} of type {@link SimpleGroup} containing all the records from given {@link InputStream}
   * @param inputStream must be of type {@link FSDataInputStream} or {@link FileInputStream}
   * @throws IOException
   */
  public  ParquetSchemaRecord getParquetRecords (InputStream inputStream)
          throws IOException {
    List<SimpleGroup> records = new ArrayList<>();

    if (!(inputStream instanceof FileInputStream)) {
      if (!(inputStream instanceof FSDataInputStream) ) {
        log.error("\u001B[31m" + "Unsupported type of InputStream {}" + "\u001B[0m"
                , inputStream.getClass().getName());
        throw new UnSupportedStreamTypeException("InputStream expected of type "
                + FileInputStream.class.getName()
                + " or " + FSDataInputStream.class.getName() + " but found "
                + inputStream.getClass().getName());
      }
    }

    log.info("\u001B[36m" + "Reading records from {}" + "\u001B[0m", inputStream.getClass().getName());

    Instant start = Instant.now();

    InputFile inputFile = new ParquetInputFile(inputStream);
    records = new ArrayList();
    ParquetFileReader reader =
            null;
    try {
      reader = ParquetFileReader.open(inputFile);
    } catch (IOException e) {
      throw new IOException("\u001B[31m" + "Error while opening Parquet Reader" + "\u001B[0m", e);
    }
    MessageType schema = reader.getFooter().getFileMetaData().getSchema();
    PageReadStore pages;
    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader recordReader = columnIO.getRecordReader(pages
              , new GroupRecordConverter(schema));

      for (int i = 0; i < rows; i++) {
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        records.add(simpleGroup);
      }
    }
    Instant finish = Instant.now();
    long timeElapsed = Duration.between(start, finish).toMillis();
    log.info("\u001B[32m" + "Returning {} records in {} milliseconds" + "\u001B[0m", records.size(), timeElapsed);
    reader.close();
    log.info("\u001B[32m" + "Reader Closed" + "\u001B[0m");
    return new ParquetSchemaRecord(records, schema);
  }
}
