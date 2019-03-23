import java.nio.file.{Path, Paths}

import gnu.trove.list.array.{TFloatArrayList, TIntArrayList}
import model.{Gene, GeneCoverage, IntervalCoverage}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
import org.jetbrains.bio.CompressionType
import org.jetbrains.bio.big._

import scala.collection.JavaConverters._
import scala.io.Source


object Main {

  val dbName = "bdgeek"
  val bamTableName = "reads"
  var chromosomeLengthMap: Map[String, Int] = _
  val sequila: SequilaSession = configureSequilaSession


  import sequila.implicits._

  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val gtfLocation = args(1)
    val sampleName = args(2)
    val coverageThreshold = args(3).toInt
    val lowCoverageRatioThreshold = args(4).toDouble
    val outputDirectory = Paths.get(args(5))
    val chromosomeLengthPath = Paths.get(args(6))

    chromosomeLengthMap = readChromosomeLengths(chromosomeLengthPath)

    outputDirectory.toFile.mkdirs()
    val coverageDS = prepareCoverageDS(bamLocation, sampleName, coverageThreshold).cache
    writeLowCoveredRegions(coverageDS, outputDirectory)
    val geneDS = prepareGeneDS(gtfLocation)
    val lowCoveredGeneDS = filterGenesWithLowCoverage(coverageDS, geneDS, lowCoverageRatioThreshold)
    val lowCoveredGenesByChromosome = lowCoveredGeneDS.rdd.groupBy(_.chromosome).collect
    writePartiallyLowCoveredGenes(lowCoveredGenesByChromosome, outputDirectory)
    writeEntirelyLowCoveredGenes(lowCoveredGenesByChromosome, outputDirectory)
  }

  def readChromosomeLengths(chrFilePath: Path): Map[String, Int] = {
      Source.fromFile(chrFilePath.toFile).getLines.map(_.split("\\s+")).map(p => p(0) -> p(1).toInt).toMap
  }

  def writeLowCoveredRegions(coverageDS: Dataset[IntervalCoverage], outputDirectory: Path): Unit = {
    val coverageByChromosome = coverageDS.rdd.groupBy(_.contigName).collect
    val outputPath = outputDirectory.resolve("low_coverage_regions.bw")
    writeToBigWig(coverageByChromosome, outputPath)
  }

  def writePartiallyLowCoveredGenes(lowCoveredGeneByChromosome: Iterable[(String, Iterable[GeneCoverage])], outputDirectory: Path): Unit = {
    val coverageByChromosome = toCoverageByChromosome(lowCoveredGeneByChromosome)
    val outputPath = outputDirectory.resolve("low_coverage_genes.bw")
    writeToBigWig(coverageByChromosome, outputPath)
  }

  def writeEntirelyLowCoveredGenes(lowCoveredGenesByChromosome: Iterable[(String, Iterable[GeneCoverage])], outputDirectory: Path): Unit = {
    val entirelyLowCoveredGenesByChromosome = lowCoveredGenesByChromosome.map(p => (p._1, p._2.filter(cov => cov.lowCoverageLength == cov.geneLength)))
    val coverageByChromosome = toCoverageByChromosome(entirelyLowCoveredGenesByChromosome)
    val outputPath = outputDirectory.resolve("low_coverage_whole_genes.bw")
    writeToBigWig(coverageByChromosome, outputPath)
  }

  private def toCoverageByChromosome(lowCoveredGeneByChromosome: Iterable[(String, Iterable[GeneCoverage])]) = {
    lowCoveredGeneByChromosome.map(p => (p._1, p._2.flatMap(_.coverageList).toSeq.sortBy(_.start)))
  }

  def configureSequilaSession: SequilaSession = {
    val spark = SparkSession.builder()
      .getOrCreate()
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    UDFRegister.register(ss)
    ss
  }

  def prepareCoverageDS(bamLocation: String, sampleName: String, coverageThreshold: Int): Dataset[IntervalCoverage] = {
    sequila.sql(s"create database if not exists $dbName")
    sequila.sql(s"use $dbName")
    sequila.sql(s"create table if not exists $bamTableName USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '$bamLocation')")
    val coverageDF = sequila.sql(s"select * from bdg_coverage('$bamTableName','$sampleName', 'blocks')").filter($"coverage" < coverageThreshold)
    coverageDF.as[IntervalCoverage]
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

  def writeToBigWig(coverageByChromosome: Iterable[(String, Iterable[IntervalCoverage])], outputPath: Path): Unit = {
    val sectionList = coverageByChromosome.map({ chrIntervals =>
      val chromosomeName = chrIntervals._1
      val intervals = chrIntervals._2
      val startList = intervals.map(_.start).toArray
      val endList = intervals.map(_.end + 1).toArray
      val coverageList = intervals.map(_.coverage.toFloat).toArray
      new BedGraphSection(chromosomeName, TIntArrayList.wrap(startList), TIntArrayList.wrap(endList), TFloatArrayList.wrap(coverageList))
    }).toList.asJava
    val chromosomeLengthArray = chromosomeLengthMap.toList.map(p => new kotlin.Pair[String, Integer](p._1, p._2)).asJava
    BigWigFile.write(sectionList, chromosomeLengthArray, outputPath, 8, CompressionType.DEFLATE)
  }

  def filterGenesWithLowCoverage(coverageDS: Dataset[IntervalCoverage], geneDS: Dataset[Gene], lowCoverageRatioThreshold: Double): Dataset[GeneCoverage] = {
    val geneStart = geneDS.col("start")
    val geneEnd = geneDS.col("end")
    val coverageStart = coverageDS.col("start")
    val coverageEnd = coverageDS.col("end")
    val rangeJoinCondition = (geneStart <= coverageEnd) && (coverageStart <= geneEnd) && ($"chromosome" === $"contigName")
    val toIntervalUDF = udf((contigName: String, start: Int, end: Int, coverage: Short) => IntervalCoverage(contigName, start, end, coverage))

    val geneCoverageDS = coverageDS.join(geneDS, rangeJoinCondition).withColumn("covStart", greatest(geneStart, coverageStart)).
      withColumn("covEnd", least(geneEnd, coverageEnd)).drop(coverageStart).drop(coverageEnd).groupBy($"geneId", $"chromosome").
      agg(first(geneEnd - geneStart).as("geneLength"), sum($"covEnd" - $"covStart").cast(IntegerType).as("lowCoverageLength"),
        collect_list(toIntervalUDF($"chromosome", $"covStart", $"covEnd", $"coverage")).as("coverageList")).as[GeneCoverage]
    val lowGeneCoverageDS = geneCoverageDS.filter($"lowCoverageLength".geq($"geneLength" * lowCoverageRatioThreshold))
    lowGeneCoverageDS
  }
}
