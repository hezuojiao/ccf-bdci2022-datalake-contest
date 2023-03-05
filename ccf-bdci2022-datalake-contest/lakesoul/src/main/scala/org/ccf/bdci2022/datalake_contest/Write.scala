package org.ccf.bdci2022.datalake_contest

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Write {
  def main(args: Array[String]): Unit = {
    val builder = SparkSession.builder()
      .appName("CCF BDCI 2022 DataLake Contest")
      .master("local[4]")
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("hadoop.fs.s3a.committer.name", "directory")
      .config("spark.hadoop.fs.s3a.committer.staging.conflict-mode", "append")
      .config("spark.hadoop.fs.s3a.committer.staging.tmp.path", "/opt/spark/work-dir/s3a_staging")
      .config("spark.hadoop.mapreduce.outputcommitter.factory.scheme.s3a", "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3.buffer.dir", "/opt/spark/work-dir/s3")
      .config("spark.hadoop.fs.s3a.buffer.dir", "/opt/spark/work-dir/s3a")
      .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
      .config("spark.hadoop.fs.s3a.fast.upload", value = true)
      .config("spark.hadoop.fs.s3a.multipart.size", 67108864)
      .config("spark.hadoop.fs.s3a.connection.maximum", 100)
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.default.parallelism", 8)
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.shuffle.compress", value = false)
      .config("spark.shuffle.spill.compress", value = false)
      .config("spark.shuffle.checksum.enabled", value = false)
      .config("spark.shuffle.detectCorrupt", value = false)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.sql.columnVector.offheap.enabled", value = true)
      .config(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key, value = true)
      .config(LakeSoulSQLConf.PART_MERGE_FILE_MINIMUM_NUM.key, value = 100)

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val tablePath = "s3://ccf-datalake-contest/datalake_table"
    val dataPath = Seq(
      "/opt/spark/work-dir/data/base-0.parquet",
      "/opt/spark/work-dir/data/base-1.parquet",
      "/opt/spark/work-dir/data/base-2.parquet",
      "/opt/spark/work-dir/data/base-3.parquet",
      "/opt/spark/work-dir/data/base-4.parquet",
      "/opt/spark/work-dir/data/base-5.parquet",
      "/opt/spark/work-dir/data/base-6.parquet",
      "/opt/spark/work-dir/data/base-7.parquet",
      "/opt/spark/work-dir/data/base-8.parquet",
      "/opt/spark/work-dir/data/base-9.parquet",
      "/opt/spark/work-dir/data/base-10.parquet"
    )

    val schema = spark.read.parquet(dataPath.head).schema
    val df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    df.write.format("lakesoul")
      .option("hashPartitions","uuid")
      .option("hashBucketNum","4")
      .mode("Overwrite")
      .save(tablePath)

    val source = dataPath.map(spark.read.parquet(_))
    val table = LakeSoulTable.forPath(tablePath)
    val newSource = Seq(
      source.head,
      mergeDataFrame(mergeDataFrame(source(1), source(2)), mergeDataFrame(source(3), source(4))),
      mergeDataFrame(mergeDataFrame(source(5), source(6)), source(7)),
      mergeDataFrame(mergeDataFrame(source(8), source(9)), source(10))
    )
    table.bulkUpsert(newSource)
  }

  private def mergeDataFrame(df1: DataFrame, df2: DataFrame): DataFrame = {
    df1.join(df2, Seq("uuid"),"full").select(
      col("uuid"),
      when(df2("ip").isNotNull, df2("ip")).otherwise(df1("ip")).alias("ip"),
      when(df2("hostname").isNotNull, df2("hostname")).otherwise(df1("hostname")).alias("hostname"),
      when(df1("requests").isNotNull && df2("requests").isNotNull, df1("requests") + df2("requests"))
        .otherwise(when(df1("requests").isNotNull, df1("requests")).otherwise(df2("requests"))).alias("requests"),
      when(df2("name").isNotNull && df2("name").notEqual("null"), df2("name")).otherwise(df1("name")).alias("name"),
      when(df2("city").isNotNull, df2("city")).otherwise(df1("city")).alias("city"),
      when(df2("job").isNotNull, df2("job")).otherwise(df1("job")).alias("job"),
      when(df2("phonenum").isNotNull, df2("phonenum")).otherwise(df1("phonenum")).alias("phonenum"))
  }
}
