# ParquetStreamReader
Library to read parquet records from InputStream

### For now only FileInputStream & FSDataInputStream implementations of InputStream are supported.

Class ```ParquetFormat``` contains a method ```getParquetRecords``` that returns List<SimpleGroup> of parquet records


## Exmaple: 
```List<SimpleGroup> list = new ParquetFormat.getParquetRecords(inputStream);```

