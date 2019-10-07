package com.parquet.format.reader;

import org.apache.avro.Schema;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.MessageType;

import java.util.List;

public class ParquetSchemaRecord {
  private List<SimpleGroup> list;
  private MessageType avroSchema;

  public List<SimpleGroup> getList () {
    return list;
  }

  public ParquetSchemaRecord (List<SimpleGroup> list , MessageType avroSchema) {
    this.list = list;
    this.avroSchema = avroSchema;
  }

  public void setList (List<SimpleGroup> list) {
    this.list = list;
  }

  public MessageType getAvroSchema () {
    return avroSchema;
  }

  public void setAvroSchema (MessageType avroSchema) {
    this.avroSchema = avroSchema;
  }
}
