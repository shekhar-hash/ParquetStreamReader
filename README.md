# ParquetStreamReader
Library to read parquet records from InputStream

### For now only FileInputStream & FSDataInputStream implementations of InputStream are supported.

Class ```ParquetFormat``` contains a method ```getParquetRecords``` that returns List<String> of parquet records


## Exmaple: 
```List<String> list = new ParquetFormat.getParquetRecords(inputStream);```

