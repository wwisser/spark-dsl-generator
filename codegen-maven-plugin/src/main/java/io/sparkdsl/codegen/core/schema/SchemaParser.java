package io.sparkdsl.codegen.core.schema;

import java.util.Set;

public interface SchemaParser<T> {
  Set<Schema> parseSchema(T source);

  SchemaSourceType getSchemaSourceType();
}
