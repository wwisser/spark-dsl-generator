package io.sparkdsl.codegen.core.schema;

public interface SchemaParser<T> {
  Schema parseSchema(T source);

  SchemaSourceType getSchemaSourceType();
}
