import org.apache.spark.sql.SparkSession

object ReadIcebergTable {
  def main(args: Array[String]): Unit = {

    val catalogName = "linkhouse_927"
    val spark = SparkSession.builder().master("local").appName("SparkOnIceberg")
      //创建Hive catalog
      //      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.%s".format(catalogName), "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.%s.type".format(catalogName), "hive")
      .config("spark.sql.catalog.%s.uri".format(catalogName), "thrift://10.201.0.212:39083")
      .config("spark.sql.defaultCatalog", catalogName)
      //Valid for Hive client 3.x
      .config("metastore.catalog.default", catalogName)
      //      .config("iceberg.engine.hive.enabled", "true")

      //指定hadoop catalog，catalog名称为hadoop_prod
      //      .config("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
      //      .config("spark.sql.catalog.hadoop_prod.type", "hadoop")
      //      .config("spark.sql.catalog.hadoop_prod.warehouse", "hdfs://nameservice1/dlink/test1/hdfs_linkhouse_103")
      .getOrCreate()

    spark.sql("show databases").show()
  }
}