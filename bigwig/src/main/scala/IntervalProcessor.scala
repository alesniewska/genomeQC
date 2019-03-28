import java.nio.file.Path

import model.{Gene, GeneCoverage, IntervalCoverage}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
import org.jetbrains.bio.big._

import scala.collection.JavaConverters._
import scala.io.Source


object IntervalProcessor {

  val dbName = "bdgeek"
  val bamTableName = "reads"
  var chromosomeLengthMap: Map[String, Int] = _
  val sequila: SequilaSession = configureSequilaSession

  import sequila.implicits._

  def readChromosomeLengths(chrFilePath: Path): Map[String, Int] = {
    Source.fromFile(chrFilePath.toFile).getLines.map(_.split("\\s+")).map(p => p(0) -> p(1).toInt).toMap
  }

  def collectByChromosome(coverageDS: Dataset[IntervalCoverage]): Array[(String, Iterable[IntervalCoverage])] = {
    coverageDS.rdd.groupBy(_.contigName).collect
  }

  def writeLowCoveredRegions(coverageDS: Dataset[IntervalCoverage], outputDirectory: Path): Unit = {
    val coverageByChromosome = collectByChromosome(coverageDS)
    val outputPath = outputDirectory.resolve("low_coverage_regions.bw")
    BigWigWriter.writeToBigWig(coverageByChromosome, outputPath)
  }

  def writePartiallyLowCoveredGenes(lowCoveredGenesByStrandChromosome: Iterable[((String, String), Iterable[GeneCoverage])], outputDirectory: Path): Unit = {
    val coverageByStrandChromosome = toCoverageByChromosome(lowCoveredGenesByStrandChromosome)
    val outputPathBuilder = (strand: String) => outputDirectory.resolve(s"low_coverage_genes_$strand.bw")
    BigWigWriter.writeTwoBigWigFiles(coverageByStrandChromosome, outputPathBuilder)
  }

  def writeEntirelyLowCoveredGenes(lowCoveredGenesByStrandChromosome: Iterable[((String, String), Iterable[GeneCoverage])], outputDirectory: Path): Unit = {
    val entirelyLowCoveredGenesByChromosome = lowCoveredGenesByStrandChromosome.map(p => (p._1, p._2.filter(cov => cov.lowCoverageLength == cov.geneLength)))
    val coverageByChromosome = toCoverageByChromosome(entirelyLowCoveredGenesByChromosome)
    val outputPathBuilder = (strand: String) => outputDirectory.resolve(s"low_coverage_whole_genes_$strand.bw")
    BigWigWriter.writeTwoBigWigFiles(coverageByChromosome, outputPathBuilder)
  }

  private def toCoverageByChromosome(lowCoveredGeneByChromosome: Iterable[((String, String), Iterable[GeneCoverage])]) = {
    lowCoveredGeneByChromosome.par.map(
      p => (p._1, p._2.filter(_.coverageList.nonEmpty).toSeq.sortBy(_.coverageList.head.start).flatMap(_.coverageList))
    ).seq
  }

  def configureSequilaSession: SequilaSession = {
    val spark = SparkSession.builder()
      .getOrCreate()
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    UDFRegister.register(ss)
    ss
  }

  def prepareCoverageDS(bamLocation: String, coverageThreshold: Int): Dataset[IntervalCoverage] = {
    sequila.sql(s"create database if not exists $dbName")
    sequila.sql(s"use $dbName")
    sequila.sql(s"create table if not exists $bamTableName USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '$bamLocation')")
    val sampleIdList = sequila.table(bamTableName).select("sampleId").distinct().as[String].collect()
    val coverageDSList = sampleIdList.map( sampleName =>
      sequila.sql(s"select * from bdg_coverage('$bamTableName','$sampleName', 'blocks')").as[IntervalCoverage].filter($"coverage" < coverageThreshold)
    )
    coverageDSList.reduceLeft(rangeJoin)
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
      filter($"feature" === "gene").select($"seqname".as("chromosome"), $"start", $"end", $"strand", geneIdExpression).as[Gene]
    geneDS
  }

  def filterGenesWithLowCoverage(coverageDS: Dataset[IntervalCoverage], geneDS: Dataset[Gene], lowCoverageRatioThreshold: Double): Dataset[GeneCoverage] = {
    val geneStart = geneDS.col("start")
    val geneEnd = geneDS.col("end")
    val coverageStart = coverageDS.col("start")
    val coverageEnd = coverageDS.col("end")
    val rangeJoinCondition = (geneStart <= coverageEnd) && (coverageStart <= geneEnd) && ($"chromosome" === $"contigName")
    val toIntervalUDF = udf((contigName: String, start: Int, end: Int, coverage: Short) => IntervalCoverage(contigName, start, end, coverage))

    // TODO: Use rangeJoin method
    val geneCoverageDS = coverageDS.join(geneDS, rangeJoinCondition).withColumn("covStart", greatest(geneStart, coverageStart)).
      withColumn("covEnd", least(geneEnd, coverageEnd)).drop(coverageStart).drop(coverageEnd).orderBy($"covStart").groupBy($"strand", $"geneId", $"chromosome").
      agg(first(geneEnd - geneStart).as("geneLength"), sum($"covEnd" - $"covStart").cast(IntegerType).as("lowCoverageLength"),
        collect_list(toIntervalUDF($"chromosome", $"covStart", $"covEnd", $"coverage")).as("coverageList")).as[GeneCoverage]
    val lowGeneCoverageDS = geneCoverageDS.filter($"lowCoverageLength".geq($"geneLength" * lowCoverageRatioThreshold))
    lowGeneCoverageDS
  }

  def intersectBigWigs(mainPath: Path, secondaryPath: Path): Dataset[IntervalCoverage] = {
    val mainBigWig = BigWigFile.read(mainPath, null)
    val secondaryBigWig = BigWigFile.read(secondaryPath, null)
    val commonChromosomes = mainBigWig.getChromosomes.values.intersect(secondaryBigWig.getChromosomes.values).map(_.toString)
    val parallelizeSections = (bwFile: BigWigFile) => {
      val sections = commonChromosomes.par.flatMap(bwFile.query(_).asScala.flatMap(section =>
        section.query.iterator().asScala.map(iv => IntervalCoverage(section.getChrom, iv.getStart, iv.getEnd, iv.getScore.toShort))
      )).seq
      sequila.sparkContext.parallelize(sections).toDS
    }
    val mainDS = parallelizeSections(mainBigWig)
    val secDS = parallelizeSections(secondaryBigWig)

    rangeJoin(mainDS, secDS)
  }


  def rangeJoin(mainDS: Dataset[IntervalCoverage], secDS: Dataset[IntervalCoverage]): Dataset[IntervalCoverage] = {
    val mainStart = mainDS.col("start")
    val mainEnd = mainDS.col("end")
    val secEnd = secDS.col("start")
    val secStart = secDS.col("end")
    val mainContig = mainDS.col("contigName")

    mainDS.join(secDS, mainStart <= secEnd && secStart <= mainEnd && mainContig === secDS.col("contigName")).
      withColumn("covStart", greatest(mainStart, secStart)).
      withColumn("covEnd", least(mainEnd, secEnd)).
      select($"covStart".as("start"), $"covEnd".as("end"), mainContig, mainDS.col("coverage")).
      as[IntervalCoverage]
  }
}
