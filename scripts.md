# docker way
docker run -it --rm  -e USERID=$UID -e GROUPID=$(id -g) -v /data/sequila:/data biodatageeks/bdg-sequila:0.5.5-spark-2.4.2-SNAPSHOT spark-shell --driver-memory=8g --jars /tmp/bdg-toolset/bdg-sequila-assembly-0.5.5-spark-2.4.2-SNAPSHOT.jar --conf spark.sql.warehouse.dir=/home/bdgeek/spark-warehouse




 # spark-shell direct way
 spark-shell --packages org.biodatageeks:bdg-sequila_2.11:0.5.5-spark-2.4.2 \
  --jars /Users/michalo/Downloads/bdg-sequila_2.11-0.5.5-spark-2.4.2-sources.jar,bdg-sequila_2.11-0.5.5-spark-2.4.2-SNAPSHOT.jar
 --conf spark.sql.warehouse.dir=/home/bdgeek/spark-warehouse --driver-memory=8g
 
 bsub -n 24 -R "rusage[mem=2560]" -Is bash
# then in  interactive job
module load java
export _JAVA_OPTIONS="-XX:ParallelGCThreads=1 -Djava.awt.headless=true -Xmx25000m"


 bsub -n 12 -R "rusage[mem=5120]" -Is bash
# then in  interactive job
module load java
export _JAVA_OPTIONS="-XX:ParallelGCThreads=1 -Djava.awt.headless=true -Xmx60000m"

 
 bsub -n 12  -R "rusage[mem=10240]" -Is bash
 # then in  interactive job
module load java
export _JAVA_OPTIONS="-XX:ParallelGCThreads=1 -Djava.awt.headless=true -Xmx120000m"
 
  ./spark-shell --packages org.biodatageeks:bdg-sequila_2.11:0.5.5-spark-2.4.2 \
  --jars bdg-sequila_2.11-0.5.5-spark-2.4.2-sources.jar,bdg-sequila_2.11-0.5.5-spark-2.4.2-SNAPSHOT.jar \
  --conf spark.sql.warehouse.dir=/cluster/home/michalo/scratch/giab --driver-memory=58g
  
    ./spark-shell --packages org.biodatageeks:bdg-sequila_2.11:0.5.5-spark-2.4.2 \
  --jars bdg-sequila_2.11-0.5.5-spark-2.4.2-sources.jar,bdg-sequila_2.11-0.5.5-spark-2.4.2-SNAPSHOT.jar \
  --conf spark.sql.warehouse.dir=/cluster/home/michalo/scratch/giab --driver-memory=118g
 
 spark-shell --packages org.biodatageeks:bdg-sequila_2.11:0.5.5-spark-2.4.2 \
  --repositories http://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/,http://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/ \
 --conf spark.sql.warehouse.dir=/data/sequila/warehouse --driver-memory=12g
  
  
 import org.apache.spark.sql._
 import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
 
 def configureSequilaSession: SequilaSession = {
    val spark = SparkSession.builder()
      .getOrCreate()
    val ss = SequilaSession(spark)
 
    ss.sqlContext.setConf("spark.biodatageeks.rangejoin.useJoinOrder","false")
       ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxBroadcastSize", (128*1024*1024).toString)
 
       ss.sqlContext.setConf("spark.biodatageeks.rangejoin.minOverlap","1")
       ss.sqlContext.setConf("spark.biodatageeks.rangejoin.maxGap","0")
 
       ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","false")
 
       ss.sqlContext.setConf("spark.biodatageeks.bam.useGKLInflate","false")
 
       ss.sqlContext.setConf("spark.biodatageeks.bam.useSparkBAM","false")
 
    SequilaRegister.register(ss)
    UDFRegister.register(ss)
 
    ss
  }
 
 val sequila = configureSequilaSession
 
 val dbName = "bdgeek"
 val bamTableName = "reads"
 
 #docker needs the path from command line
 val bamLocation = "/data/NA12878.slice.bam"
 val bamLocation = "/data/sequila/NA12878.slice.bam"
 val bamLocation = "/data/sequila/NA12878_phased_possorted_bam.bam"
 val bamLocation = "/cluster/home/michalo/scratch/giab/NA12878_phased_possorted_bam.bam"
 val bamLocation = "/cluster/home/michalo/scratch/giab/NA12878_chr1.bam"
 
 
 # any bam in freestyle mode
 val bamLocation = "/Users/michalo/projects/sequila/KPC530_Nx_L004.bam"
  val bamLocation = "/cluster/home/michalo/scratch/giab/KPC530_Nx_L004.bam"
 
 sequila.sql(s"create database if not exists $dbName")
 sequila.sql(s"use $dbName")
 sequila.sql(s"drop table if exists $bamTableName")
 sequila.sql(s"create table $bamTableName USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '$bamLocation')")
 val sampleName = sequila.table(bamTableName).select("sampleId").as[String].head
 val c = sequila.sql(s"select * from bdg_coverage('$bamTableName','$sampleName', 'blocks')").cache.count
 sequila.sql(s"select * from bdg_coverage('$bamTableName','$sampleName', 'blocks')").show(10)
 
 
 val outt = sequila.sql(s"select * from bdg_coverage('$bamTableName','$sampleName', 'blocks')")
 outt.write.format("com.databricks.spark.csv").save("/cluster/home/michalo/scratch/giab/cov_giab_full_120GBmem.csv")




 
