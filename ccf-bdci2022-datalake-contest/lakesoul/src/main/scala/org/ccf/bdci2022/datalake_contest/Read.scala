package org.ccf.bdci2022.datalake_contest

import com.dmetasoul.lakesoul.tables.LakeSoulTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.{MergeNonNullOp, MergeOpLong}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.lakesoul.sources.LakeSoulSQLConf

object Read {
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
      .config("spark.sql.parquet.mergeSchema", value = false)
      .config("spark.sql.parquet.filterPushdown", value = true)
      .config("spark.shuffle.compress", value = false)
      .config("spark.shuffle.spill.compress", value = false)
      .config("spark.shuffle.checksum.enabled", value = false)
      .config("spark.shuffle.detectCorrupt", value = false)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", 4)
      .config("spark.default.parallelism", 8)
      .config("spark.sql.files.maxPartitionBytes", "1g")
      .config("spark.hadoop.mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")
      .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
      .config("spark.sql.warehouse.dir", "s3://ccf-datalake-contest/datalake_table/")
      .config("spark.sql.extensions", "com.dmetasoul.lakesoul.sql.LakeSoulSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog")
      .config("spark.sql.columnVector.offheap.enabled", value = true)
      .config("spark.sql.parquet.columnarReaderBatchSize", value = 8192)
      .config(LakeSoulSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE.key, value = true)
      .config(LakeSoulSQLConf.PART_MERGE_FILE_MINIMUM_NUM.key, value = 100)

    if (args.length >= 1 && args(0) == "--localtest")
      builder.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider")

    val spark = builder.getOrCreate()
    new MergeOpLong().register(spark, "reqOp")
    new MergeNonNullOp[String]().register(spark, "nameOp")

    spark.sparkContext.setLogLevel("ERROR")
    val tablePath= "s3://ccf-datalake-contest/datalake_table"
    val table = LakeSoulTable.forPath(tablePath)
    table.toDF
      .select("uuid", "ip", "hostname", "requests", "name", "city", "job", "phonenum")
      .withColumn("requests", expr("reqOp(requests)"))
      .withColumn("name", expr("nameOp(name)"))
      .select("uuid", "ip", "hostname", "requests", "name", "city", "job", "phonenum")
      .write.parquet("/opt/spark/work-dir/result/ccf/")
  }
}
