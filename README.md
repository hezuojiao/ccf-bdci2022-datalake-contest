# README

## 1. 代码结构

本次比赛采用的数据湖框架为`LakeSoul` , 代码修改基于`tag: 2.1.0` , 里面包括了针对比赛场景的写入优化、读取优化、`Spark` 内核优化等；

`ccf-bdci2022-datalake-contest` 目录是比赛的提交代码，主要是参数调优和`Partial Merge` ;


所有代码通过`git`提交，可以使用`git show` 命令跟踪代码修改，进行`code review`。

**更新**:  
提交至github时删除了原项目的git日志，差异点请关注  
https://github.com/hezuojiao/ccf-bdci2022-datalake-contest/blob/main/LakeSoul/ccf-bdci2022-datalake-contest.diff  
https://github.com/hezuojiao/ccf-bdci2022-datalake-contest/blob/main/ccf-bdci2022-datalake-contest/ccf-bdci2022-datalake-contest.diff  

## 2. 方案介绍

### 2.1 写优化

本次比赛需要写入(`Upsert`) 0 ~ 10 共11个文件，如果串行提交会导致executor空闲，无法得到充分利用。所以这里可以采用并行提交写Job，再控制commit顺序来保证按照顺序Upsert语义。

- `LakeSoulTable` 新增`bulkUpsert` 接口，并为这个接口实现单独的`Command` ;
- 将写文件的代码从`TransactionalWrite` 抽离出来，提供不依赖`Transaction` 的写任务提交；
- 并发提交写文件的任务，再按照顺序做`Transaction commit` ,把写入成功的文件保存在meta数据库中；



### 2.2 读优化

针对比赛写多读少的特点，这里选择的方案是`Merge On Read` , 分析当前框架存在的性能问题:

1. Merge Operator的语义是把多版本的数据合并成一个数据，本质上是一个scalar操作，读取时没必要为每个版本保存一份数据，再做同一的合并；
2. 由于需要保存多版本数据，所以对数据进行了多次深拷贝，本次比赛涉及多个String字段，数据拷贝很容易成为瓶颈点。



针对比赛场景，实现了`FastMerge` , 目标是尽可能少产生java对象和数据拷贝；

1. 实现本次比赛中所需的`Update in place` 的合并操作;
2. 调整堆排序的顺序，首先按照主键排序、再按照**高版本在前**的顺序排列，一般来说，高版本作为最新的数据，不太会需要更新；
3. 对于每一行数据，内存生命周期是到`nextBatch` 结束，所以在这之前都只做浅拷贝，当前文件发生`nextBatch`前做深拷贝；
4. 如果是`Add` 操作，由于本次比赛`add`字段是`Long`，所以不需要拷贝数据，直接求和更新即可；
5. 如果是`NonNull` 操作,只有当高版本的数据为`NULL`时才需要更新，更新时需要做深拷贝，其它字段都不需要做拷贝。



### 2.3 Spark内核优化

- `SortMergeJoin`优化

根据`Upsert`语义, 本质上是一个对非空主键的`Full Outer Join`，比赛的数据量基本都是走了`SortMergeJoin` ,当然也可以走`ShuffleHashJoin` ，但是`spark 3.1.2`的`ShuffleHashJoin` 还不是很成熟，而且`Merge`写入的数据要求有序，所以`SortMergeJoin`是更合适的选择。

3.1.2版本的`SortMergeJoin`算子没有实现本赛场景需要的`CodeGen`, 性能会比较差，所以这里参考https://github.com/apache/spark/pull/34581 在3.1.2实现了`full outer sort merge join`的`codegen` 。



- 计划优化(未验证)

在`spark`中，如果想对三个以上的`DataFrame`做`Merge`，会对已经做了`SortMergeJoin`后的结果再做重复的`Exchange`和`Sort`, 原因是spark无法感知主键非null的特征:

```scala
== Physical Plan ==
*(9) Project [coalesce(uuid#176, uuid#48) AS uuid#207, CASE WHEN isnotnull(ip#49) THEN ip#49 ELSE ip#192 END AS ip#223, CASE WHEN isnotnull(hostname#50) THEN hostname#50 ELSE hostname#193 END AS hostname#224, CASE WHEN (isnotnull(requests#194L) AND isnotnull(requests#51L)) THEN (requests#194L + requests#51L) ELSE CASE WHEN isnotnull(requests#194L) THEN requests#194L ELSE requests#51L END END AS requests#225L, CASE WHEN (isnotnull(name#52) AND NOT (name#52 = null)) THEN name#52 ELSE name#195 END AS name#226, CASE WHEN isnotnull(city#53) THEN city#53 ELSE city#196 END AS city#227, CASE WHEN isnotnull(job#54) THEN job#54 ELSE job#197 END AS job#228, CASE WHEN isnotnull(phonenum#55) THEN phonenum#55 ELSE phonenum#198 END AS phonenum#229]
+- *(9) SortMergeJoin [uuid#176], [uuid#48], FullOuter
   :- *(6) Sort [uuid#176 ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(uuid#176, 5), ENSURE_REQUIREMENTS, [id=#76]
   :     +- *(5) Project [coalesce(uuid#16, uuid#32) AS uuid#176, CASE WHEN isnotnull(ip#33) THEN ip#33 ELSE ip#17 END AS ip#192, CASE WHEN isnotnull(hostname#34) THEN hostname#34 ELSE hostname#18 END AS hostname#193, CASE WHEN (isnotnull(requests#19L) AND isnotnull(requests#35L)) THEN (requests#19L + requests#35L) ELSE CASE WHEN isnotnull(requests#19L) THEN requests#19L ELSE requests#35L END END AS requests#194L, CASE WHEN (isnotnull(name#36) AND NOT (name#36 = null)) THEN name#36 ELSE name#20 END AS name#195, CASE WHEN isnotnull(city#37) THEN city#37 ELSE city#21 END AS city#196, CASE WHEN isnotnull(job#38) THEN job#38 ELSE job#22 END AS job#197, CASE WHEN isnotnull(phonenum#39) THEN phonenum#39 ELSE phonenum#23 END AS phonenum#198]
   :        +- *(5) SortMergeJoin [uuid#16], [uuid#32], FullOuter
   :           :- *(2) Sort [uuid#16 ASC NULLS FIRST], false, 0
   :           :  +- Exchange hashpartitioning(uuid#16, 5), ENSURE_REQUIREMENTS, [id=#60]
   :           :     +- *(1) ColumnarToRow
   :           :        +- FileScan parquet [uuid#16,ip#17,hostname#18,requests#19L,name#20,city#21,job#22,phonenum#23] Batched: true, DataFilters: [], Format: Parquet
   :           +- *(4) Sort [uuid#32 ASC NULLS FIRST], false, 0
   :              +- Exchange hashpartitioning(uuid#32, 5), ENSURE_REQUIREMENTS, [id=#68]
   :                 +- *(3) ColumnarToRow
   :                    +- FileScan parquet [uuid#32,ip#33,hostname#34,requests#35L,name#36,city#37,job#38,phonenum#39] Batched: true, DataFilters: [], Format: Parquet
   +- *(8) Sort [uuid#48 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(uuid#48, 5), ENSURE_REQUIREMENTS, [id=#84]
         +- *(7) ColumnarToRow
            +- FileScan parquet [uuid#48,ip#49,hostname#50,requests#51L,name#52,city#53,job#54,phonenum#55] Batched: true, DataFilters: [], Format: Parquet

```

利用主键非NULL且唯一的特征，`coalesce(uuid#16, uuid#32) AS uuid#176` 的结果应该是符合分区有序的特征，消除`Exchange`和`Sort` 后计划:

```
== Physical Plan ==
*(8) Project [coalesce(uuid#176, uuid#48) AS uuid#207, CASE WHEN isnotnull(ip#49) THEN ip#49 ELSE ip#192 END AS ip#223, CASE WHEN isnotnull(hostname#50) THEN hostname#50 ELSE hostname#193 END AS hostname#224, CASE WHEN (isnotnull(requests#194L) AND isnotnull(requests#51L)) THEN (requests#194L + requests#51L) ELSE CASE WHEN isnotnull(requests#194L) THEN requests#194L ELSE requests#51L END END AS requests#225L, CASE WHEN (isnotnull(name#52) AND NOT (name#52 = null)) THEN name#52 ELSE name#195 END AS name#226, CASE WHEN isnotnull(city#53) THEN city#53 ELSE city#196 END AS city#227, CASE WHEN isnotnull(job#54) THEN job#54 ELSE job#197 END AS job#228, CASE WHEN isnotnull(phonenum#55) THEN phonenum#55 ELSE phonenum#198 END AS phonenum#229]
+- *(8) SortMergeJoin [uuid#176], [uuid#48], FullOuter
   :- *(5) Project [coalesce(uuid#16, uuid#32) AS uuid#176, CASE WHEN isnotnull(ip#33) THEN ip#33 ELSE ip#17 END AS ip#192, CASE WHEN isnotnull(hostname#34) THEN hostname#34 ELSE hostname#18 END AS hostname#193, CASE WHEN (isnotnull(requests#19L) AND isnotnull(requests#35L)) THEN (requests#19L + requests#35L) ELSE CASE WHEN isnotnull(requests#19L) THEN requests#19L ELSE requests#35L END END AS requests#194L, CASE WHEN (isnotnull(name#36) AND NOT (name#36 = null)) THEN name#36 ELSE name#20 END AS name#195, CASE WHEN isnotnull(city#37) THEN city#37 ELSE city#21 END AS city#196, CASE WHEN isnotnull(job#38) THEN job#38 ELSE job#22 END AS job#197, CASE WHEN isnotnull(phonenum#39) THEN phonenum#39 ELSE phonenum#23 END AS phonenum#198]
   :  +- *(5) SortMergeJoin [uuid#16], [uuid#32], FullOuter
   :     :- *(2) Sort [uuid#16 ASC NULLS FIRST], false, 0
   :     :  +- Exchange hashpartitioning(uuid#16, 5), ENSURE_REQUIREMENTS, [id=#60]
   :     :     +- *(1) ColumnarToRow
   :     :        +- FileScan parquet [uuid#16,ip#17,hostname#18,requests#19L,name#20,city#21,job#22,phonenum#23] Batched: true, DataFilters: [], Format: Parquet
   :     +- *(4) Sort [uuid#32 ASC NULLS FIRST], false, 0
   :        +- Exchange hashpartitioning(uuid#32, 5), ENSURE_REQUIREMENTS, [id=#68]
   :           +- *(3) ColumnarToRow
   :              +- FileScan parquet [uuid#32,ip#33,hostname#34,requests#35L,name#36,city#37,job#38,phonenum#39] Batched: true, DataFilters: [], Format: Parquet
   +- *(7) Sort [uuid#48 ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(uuid#48, 5), ENSURE_REQUIREMENTS, [id=#80]
         +- *(6) ColumnarToRow
            +- FileScan parquet [uuid#48,ip#49,hostname#50,requests#51L,name#52,city#53,job#54,phonenum#55] Batched: true, DataFilters: [], Format: Parquet
```

可惜这个想法实现时只剩下最后一次提交，只在本地的suite测试中发现规则生效了，提交完发现规则未生效。



基于这里所做的spark内核优化，实测发现本地做Partial Merge比直接写S3更快，所以在Write阶段会做一部分的Partial Merge，提高读写性能。



### 2.4 参数优化

```
-- 本地任务去掉shuffle压缩来节省cpu
("spark.shuffle.compress", value = false)
("spark.shuffle.spill.compress", value = false)
("spark.shuffle.checksum.enabled", value = false)
("spark.shuffle.detectCorrupt", value = false)

-- 序列化
("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

-- offheap读
("spark.sql.columnVector.offheap.enabled", value = true)

-- committer算法版本
("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")

```


