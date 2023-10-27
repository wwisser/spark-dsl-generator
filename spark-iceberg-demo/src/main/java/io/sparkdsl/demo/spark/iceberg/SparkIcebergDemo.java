package io.sparkdsl.demo.spark.iceberg;

import static java.util.Arrays.asList;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkIcebergDemo {

  private static final String HADOOP_HOME = "C:\\Users\\Wendelin\\hadoop";
  private static final String DEFAULT_FS = "file:///";

  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", HADOOP_HOME);

    Configuration configuration = new Configuration();
    configuration.set("fs.defaultFS", DEFAULT_FS);

    SparkSession spark = SparkSession.builder()
        .appName("Java Spark Iceberg Demo")
        .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.my_catalog.type", "hadoop")
        .config("spark.sql.catalog.my_catalog.warehouse", "file:///tmp/iceberg_warehouse")
        .master("local[*]")
        .getOrCreate();

    Schema schema = new Schema(
        Types.NestedField.required(1, "first_name", Types.StringType.get()),
        Types.NestedField.required(2, "last_name", Types.StringType.get()),
        Types.NestedField.required(3, "age", Types.IntegerType.get())
    );

    HadoopCatalog catalog = new HadoopCatalog(spark.sparkContext().hadoopConfiguration(),
        "/tmp/iceberg_warehouse");

    TableIdentifier name = TableIdentifier.of("my_catalog", "default", "users");

    try {
      catalog.createTable(name, schema);
    } catch (AlreadyExistsException e) {
      System.err.println("table already exists: " +  e.getMessage());
    }

    StructType dataSchema = new StructType()
        .add("first_name", DataTypes.StringType)
        .add("last_name", DataTypes.StringType)
        .add("age", DataTypes.IntegerType);

    Dataset<Row> data = spark.createDataFrame(asList(
        RowFactory.create("John", "Doe", 30),
        RowFactory.create("Jane", "Doe", 25)
    ), dataSchema);

    data.write().format("iceberg").mode("append").save("default.users");

    Dataset<Row> result = spark.read().format("iceberg").load("default.users");
    result.show();

    spark.stop();
  }

}
