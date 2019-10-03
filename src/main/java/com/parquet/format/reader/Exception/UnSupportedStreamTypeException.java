package com.parquet.format.reader.Exception;

import javax.management.remote.SubjectDelegationPermission;
import java.io.IOException;

public class UnSupportedStreamTypeException extends IOException {
  public UnSupportedStreamTypeException(String cause) {
    super(cause);
  }
}
