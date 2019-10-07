package com.parquet.format.reader.Exception;

import org.apache.parquet.example.data.simple.SimpleGroup;

import javax.xml.validation.Schema;
import java.util.List;

public class ParquetSchemaRecord {
  private List<SimpleGroup> list;
  private Schema avroSchema;

  public List<SimpleGroup> getList () {
    return list;
  }

  public ParquetSchemaRecord (List<SimpleGroup> list , Schema avroSchema) {
    this.list = list;
    this.avroSchema = avroSchema;
  }

  public void setList (List<SimpleGroup> list) {
    this.list = list;
  }

  public Schema getAvroSchema () {
    return avroSchema;
  }

  public void setAvroSchema (Schema avroSchema) {
    this.avroSchema = avroSchema;
  }
}
