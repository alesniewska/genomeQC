import java.io.PrintWriter
import java.nio.file.Path

import model._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
import org.jetbrains.bio.big._

import scala.collection.JavaConverters._

class IntervalProcessor(chromosomeLengthPath: Path, outputDirectory: Path) {

  val CHR_PREFIX = "chr"

  val dbName = "bdgeek"
  val bamTableName = "reads"
  val sequila: SequilaSession = configureSequilaSession
  val bigWigWriter = new BigWigWriter(chromosomeLengthPath)

  import sequila.implicits._

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
    ss.sqlContext.setConf("spark.sql.broadcastTimeout", "36000")
    SequilaRegister.register(ss)
    UDFRegister.register(ss)
    ss
  }


  def collectByChromosome(coverageDS: Dataset[SimpleInterval]): Array[(String, Iterable[SimpleInterval])] = {
    coverageDS.rdd.groupBy(_.contigName).collect
  }

  def writeLowCoveredRegions(coverageDS: Dataset[SimpleInterval], sampleName: String): Unit = {
    val coverageByChromosome = collectByChromosome(coverageDS)
    val outputPath = outputDirectory.resolve(s"low_coverage_regions_$sampleName.bw")
    bigWigWriter.writeToBigWig(coverageByChromosome, outputPath)
  }

  def writePartiallyLowCoveredGenes(lowCoveredGenesByStrandChromosome: Iterable[((String, String), Iterable[GeneCoverage])]): Unit = {
    val coverageByStrandChromosome = toCoverageByChromosome(lowCoveredGenesByStrandChromosome)
    val outputPathBuilder = (strand: String) => outputDirectory.resolve(s"low_coverage_genes_$strand.bw")
    bigWigWriter.writeTwoBigWigFiles(coverageByStrandChromosome, outputPathBuilder)
  }

  def writeEntirelyLowCoveredGenes(lowCoveredGenesByStrandChromosome: Iterable[((String, String), Iterable[GeneCoverage])]): Unit = {
    val entirelyLowCoveredGenesByChromosome = lowCoveredGenesByStrandChromosome.map(p => (p._1, p._2.filter(cov => cov.lowCoverageLength == cov.geneLength)))
    val coverageByChromosome = toCoverageByChromosome(entirelyLowCoveredGenesByChromosome)
    val outputPathBuilder = (strand: String) => outputDirectory.resolve(s"low_coverage_whole_genes_$strand.bw")
    bigWigWriter.writeTwoBigWigFiles(coverageByChromosome, outputPathBuilder)
  }

  private def toCoverageByChromosome(lowCoveredGeneByChromosome: Iterable[((String, String), Iterable[GeneCoverage])]) = {
    lowCoveredGeneByChromosome.par.map(
      p => (p._1, p._2.filter(_.coverageList.nonEmpty).toSeq.sortBy(_.coverageList.head.start).flatMap(_.coverageList))
    ).seq
  }

  def prepareLowCoverageSamples(bamLocation: String, coverageThreshold: Int): List[(Dataset[SimpleInterval], String)] = {
    sequila.sql(s"create database if not exists $dbName")
    sequila.sql(s"use $dbName")
    sequila.sql(s"drop table if exists $bamTableName")
    sequila.sql(s"create table $bamTableName USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '$bamLocation')")
    val sampleIdList = sequila.table(bamTableName).select("sampleId").distinct().as[String].collect()
    sampleIdList.map(sampleName =>
      (sequila.sql(s"select * from bdg_coverage('$bamTableName','$sampleName', 'blocks')").
        filter($"coverage" < coverageThreshold).drop("coverage").as[SimpleInterval], sampleName)
    ).toList
  }

  def prepareGeneDS(gtfLocation: String): Dataset[Gene] = {
    val gtfSchema = StructType(Array(
      StructField("seqname", StringType, nullable = false), StructField("source", StringType, nullable = false),
      StructField("feature", StringType, nullable = false), StructField("start", IntegerType, nullable = false),
      StructField("end", IntegerType, nullable = false), StructField("score", StringType, nullable = false),
      StructField("strand", StringType, nullable = false), StructField("frame", StringType, nullable = false),
      StructField("attributes", StringType, nullable = false)
    ))
    val geneIdExpression = regexp_extract($"attributes", """.*gene_id\s"([^"]+)";""", 1).as("geneId")
    val geneDS = sequila.read.option("sep", "\t").option("comment", "#").schema(gtfSchema).csv(gtfLocation).
      select("seqname", "feature", "start", "end", "strand", "attributes").
      filter($"feature" === "gene")
      .select(withChrPrefix($"seqname").as("contigName"), $"start", $"end", $"strand", geneIdExpression)
      .as[Gene]
    geneDS
  }

  def withChrPrefix(contigName: Column): Column = {
    when(contigName.startsWith(CHR_PREFIX), contigName)
      .otherwise(concat(lit(CHR_PREFIX), contigName))
  }

  def withChrPrefix(contigName: String): String = {
    if (contigName.startsWith(CHR_PREFIX)) {
      contigName
    } else {
      CHR_PREFIX + contigName
    }
  }

  def filterGenesWithLowCoverage(coverageDS: Dataset[SimpleInterval], geneDS: Dataset[Gene], lowCoverageRatioThreshold: Double): Dataset[GeneCoverage] = {
    val toIntervalUDF = udf((chromosome: String, start: Int, end: Int) => SimpleInterval(chromosome, start, end))

    val geneWithLengthDS = geneDS.withColumn("geneLength", $"end" - $"start").as[Gene]

    val geneCoverageDS = rangeJoin(coverageDS, geneWithLengthDS).
      orderBy($"start").withColumnRenamed("contigName", "chromosome").groupBy($"strand", $"geneId", $"chromosome").
      agg(first($"geneLength").as("geneLength"), sum($"end" - $"start").cast(IntegerType).as("lowCoverageLength"),
        collect_list(toIntervalUDF($"chromosome", $"start", $"end")).as("coverageList")).as[GeneCoverage]
    val lowGeneCoverageDS = geneCoverageDS.filter($"lowCoverageLength".geq($"geneLength" * lowCoverageRatioThreshold))
    lowGeneCoverageDS
  }

  def loadMappabilityTrack(mappabilityPath: Path, mappabilityThreshold: Double): Dataset[SimpleInterval] = {
    def mappabilityIntervals = loadBigWig(mappabilityPath)
    def selectedIntervals = mappabilityIntervals.filter(_.annotation >= mappabilityThreshold)
      .map(interval => SimpleInterval(interval.contigName, interval.start, interval.end)).toSeq
    sequila.sparkContext.parallelize(selectedIntervals).toDS
  }

  def loadBigWig(bigWigPath: Path): Iterable[AnnotatedInterval] = {
    val bigWigFile = BigWigFile.read(bigWigPath, null)
    val sections = bigWigFile.getChromosomes.values.par.flatMap(chr => bigWigFile.query(chr.toString).asScala.flatMap(section =>
      section.query.iterator().asScala.map(iv => {
        AnnotatedInterval(withChrPrefix(section.getChrom), iv.getStart, iv.getEnd, iv.getScore)
      })
    )).seq
    sections
  }


  def rangeJoin(firstDS: Dataset[_ <: ContigInterval], secondDS: Dataset[_ <: ContigInterval]): DataFrame = {
    val firstStart = firstDS.col("start")
    val firstEnd = firstDS.col("end")
    val secStart = secondDS.col("start")
    val secEnd = secondDS.col("end")

    val result = firstDS.join(secondDS, "contigName").
      where(firstStart <= secEnd && secStart <= firstEnd).
      withColumn("intStart", greatest(firstStart, secStart)).
      withColumn("intEnd", least(firstEnd, secEnd)).
      drop("start", "end").
      withColumnRenamed("intStart", "start").
      withColumnRenamed("intEnd", "end")
    result
  }

  def simpleRangeJoinList(dsList: List[Dataset[_ <: ContigInterval]]): Dataset[SimpleInterval] = {
    val rangeJoinReduce = (firstDS: Dataset[SimpleInterval], secondDS: Dataset[SimpleInterval]) => {
      rangeJoin(firstDS, secondDS).as[SimpleInterval]
    }
    dsList.map(_.as[SimpleInterval]).reduceLeft(rangeJoinReduce)
  }

  def writeGeneSummary(geneList: Iterable[GeneCoverage]): Unit = {
    val calcGeneCovRatio = (gene: GeneCoverage)=> 1.0 * gene.lowCoverageLength / gene.geneLength
    val writer = new PrintWriter(outputDirectory.resolve("gene_summary.txt").toFile)
    try {
      writer.println("chromosome,strand,gene,coverage")

      geneList.foreach(gene => {
        val line = "%s,%s,%s,%.2f".format(gene.chromosome, gene.strand, gene.geneId, calcGeneCovRatio(gene))
        writer.println(line)
      })
    } finally {
      writer.close()
    }
  }
}
