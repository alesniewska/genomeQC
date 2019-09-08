package genomeqc

import java.nio.file.Path

import genomeqc.model._
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.biodatageeks.utils.{SequilaRegister, UDFRegister}
import org.jetbrains.bio.big._

import scala.collection.JavaConverters._

class IntervalProcessor(outputDirectory: Path) {

  private lazy val logger = LogManager.getLogger(getClass)

  val CHR_PREFIX = "chr"
  val DB_NAME = "bdgeek"
  val BAM_TABLE_NAME = "reads"

  val sequila: SequilaSession = configureSequilaSession
  val resultWriter = new ResultWriter(sequila)

  import sequila.implicits._

  def configureSequilaSession: SequilaSession = {
    logger.debug("Initializing Sequila")
    val spark = SparkSession.builder()
      .getOrCreate()
    val ss = SequilaSession(spark)
    ss.sqlContext.setConf("spark.biodatageeks.bam.predicatePushdown","true")
    ss.sqlContext.setConf("spark.sql.broadcastTimeout", "36000")
    SequilaRegister.register(ss)
    UDFRegister.register(ss)
    logger.debug("Finished initializing Sequila")
    ss
  }

  /**
    * @param coverageFilter Function filtering intervals based on value of coverage.
    */
  def writeRegionsAndIntersection(bamLocation: String, coverageFilter: Column => Column): Dataset[SimpleInterval] = {
    logger.debug("Loading BAM files")
    val sampleDSList = prepareCoverageSamples(bamLocation).
      map(p => p._1.filter(coverageFilter($"coverage")).cache.as[SimpleInterval] -> p._2)
    sampleDSList.foreach(p => logDiagnosticInfo(p._1, p._2))
    val mergeIntervalsBC = sequila.sparkContext.broadcast(resultWriter.mergeIntervals)
    val sampleDSList.map(p => p._1.mapPartitions(part => mergeIntervalsBC.value(part)) -> p._2)
    sampleDSList.foreach(p => writeCoverageRegions(p._1, p._2))
    if (sampleDSList.size > 1) {
      logger.debug("Computing intersection of low coverage interval across samples")
      val lowCoverageDS = simpleRangeJoinList(sampleDSList.map(_._1)).cache()
      logDiagnosticInfo(lowCoverageDS, "bam_intersection")
      writeCoverageRegions(lowCoverageDS, "intersection")
      sampleDSList.foreach(_._1.unpersist)
      lowCoverageDS
    } else {
      logger.debug("Only one bam file provided - skipping computing intersection")
      sampleDSList.head._1
    }
  }

  private def logDiagnosticInfo(ds: Dataset[_], dsName: String): Unit = {
    if (logger.isInfoEnabled) {
      val partitionCount = ds.rdd.getNumPartitions
      logger.log(Level.INFO, s"$dsName, number of partitions: $partitionCount")
      val partitionSizes = ds.rdd.mapPartitions(part => Iterator(part.size)).collect
      val totalSize = partitionSizes.sum
      val partitionSizesStr = partitionSizes.mkString(", ")
      logger.log(Level.INFO, s"$dsName, partition row distribution: [$partitionSizesStr] (total $totalSize)")
    }
  }

  def writeCoverageRegions(coverageDS: Dataset[SimpleInterval], sampleName: String): Unit = {
    val outputPath = outputDirectory.resolve(s"coverage_regions_$sampleName")
    logger.debug(s"Saving $sampleName")
    resultWriter.writeResultToBedFile(coverageDS, outputPath)
    logger.debug(s"Finished saving $sampleName")
  }

  def writeCoverageStrandedRegions(coverageDS: Dataset[StrandedInterval], outputPathBuilder: String => Path): Unit = {
    resultWriter.writePairOfResults(coverageDS, outputPathBuilder)
  }

  def writeLowCoveredGenes(lowCoveredGeneDS: Dataset[GeneCoverage]): Unit = {
    val geneIntervalDF = lowCoveredGeneDS.flatMap(gene => gene.coverageList.map(i =>
      GeneInterval(i.contigName, i.start, i.end, gene.strand, gene.geneId, gene.lowCoverageLength, gene.geneLength))).cache()
    logDiagnosticInfo(geneIntervalDF, "gene_coverage")
    val entirelyLowCoveredGeneDS = geneIntervalDF.filter($"lowCoverageLength" === $"geneLength").as[StrandedInterval]

    val lowCoverageOutputPathBuilder = (strand: String) => outputDirectory.resolve(s"low_coverage_genes_$strand")
    val entireLowCoverageOutputPathBuilder = (strand: String) => outputDirectory.resolve(s"low_coverage_whole_genes_$strand")
    val geneSummaryPath = outputDirectory.resolve("gene_summary")

    logger.debug("Writing low covered genes")
    resultWriter.writePairOfResults(geneIntervalDF.as[StrandedInterval], lowCoverageOutputPathBuilder)
    logger.debug("Filtering and writing entirely low covered genes")
    resultWriter.writePairOfResults(entirelyLowCoveredGeneDS, entireLowCoverageOutputPathBuilder)
    logger.debug("Writing gene summary")
    resultWriter.writeGeneSummary(geneIntervalDF.toDF, geneSummaryPath)
  }

  def prepareHighCoverageSamples(bamLocation: String, coverageThreshold: Int): List[(Dataset[SimpleInterval], String)] = {
    prepareCoverageSamples(bamLocation).map( p =>
      p._1.filter($"coverage" > coverageThreshold).drop("coverage").as[SimpleInterval] -> p._2
    )
  }

  def prepareCoverageSamples(bamLocation: String): List[(Dataset[SimpleInterval], String)] = {
    sequila.sql(s"create database if not exists $DB_NAME")
    sequila.sql(s"use $DB_NAME")
    sequila.sql(s"drop table if exists $BAM_TABLE_NAME")
    sequila.sql(s"create table $BAM_TABLE_NAME USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '$bamLocation')")
    val sampleIdList = sequila.table(BAM_TABLE_NAME).select("sampleId").distinct().as[String].collect()
    sampleIdList.map(sampleName =>
      (sequila.sql(s"select * from bdg_coverage('$BAM_TABLE_NAME','$sampleName', 'blocks')").
        select(withChrPrefix($"contigName").as("contigName"), $"start", $"end").as[SimpleInterval], sampleName)
    ).toList
  }

  def prepareGeneDS(gtfLocation: String): Dataset[Gene] = {
    logger.debug("Loading gene data")
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
      .as[Gene].cache()
    logDiagnosticInfo(geneDS, "genes")
    geneDS
  }

  def prepareExonDS(gtfLocation: String): DataFrame = {
    // TODO: DRY
    logger.debug("Loading exon data")
    val gtfSchema = StructType(Array(
      StructField("seqname", StringType, nullable = false), StructField("source", StringType, nullable = false),
      StructField("feature", StringType, nullable = false), StructField("start", IntegerType, nullable = false),
      StructField("end", IntegerType, nullable = false), StructField("score", StringType, nullable = false),
      StructField("strand", StringType, nullable = false), StructField("frame", StringType, nullable = false),
      StructField("attributes", StringType, nullable = false)
    ))
    val geneIdExpression = regexp_extract($"attributes", """.*gene_id\s"([^"]+)";""", 1).as("geneId")
    val exonIdExpression = regexp_extract($"attributes",  """.*exon_id\s"([^"]+)";""", 1).as("exonId")
    val exonDS = sequila.read.option("sep", "\t").option("comment", "#").schema(gtfSchema).csv(gtfLocation).
      select("seqname", "feature", "start", "end", "strand", "attributes").
      filter($"feature" === "exon").
      select(withChrPrefix($"seqname").as("contigName"), $"start", $"end", $"strand",
        geneIdExpression, exonIdExpression).cache()
    logDiagnosticInfo(exonDS, "exons")
    exonDS
  }

  def prepareGeneLengthDF(exonDS: Dataset[SimpleInterval]): DataFrame = {
    val geneLengthDF = exonDS.groupBy($"geneId").agg(sum($"end" - $"start").as("exonLengthSum")).cache
    logDiagnosticInfo(geneLengthDF, "gene_length")
    geneLengthDF
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
    logger.debug("Filtering low coverage genes")
    val toIntervalUDF = udf((contigName: String, start: Int, end: Int) => SimpleInterval(contigName, start, end))

    val geneWithLengthDS = geneDS.withColumn("geneLength", $"end" - $"start").as[Gene]

    val geneCoverageDS = rangeJoin(coverageDS, geneWithLengthDS).
      orderBy($"start").groupBy($"strand", $"geneId", $"contigName").
      agg(first($"geneLength").as("geneLength"), sum($"end" - $"start").cast(IntegerType).as("lowCoverageLength"),
        collect_list(toIntervalUDF($"contigName", $"start", $"end")).as("coverageList")).as[GeneCoverage]
    val lowGeneCoverageDS = geneCoverageDS.filter($"lowCoverageLength".geq($"geneLength" * lowCoverageRatioThreshold))
    lowGeneCoverageDS
  }

  def filterEntirelyCoveredIntervals(coverageIntervalsDS: Dataset[_ <: ContigInterval],
                                     intervalComplexId: String*): DataFrame = {
    logger.debug("Filtering entirely covered intervals")
    val aggKeyColumns = intervalComplexId.map(col)

    coverageIntervalsDS.groupBy(aggKeyColumns: _*).agg(
      first($"start").as("start"), first($"end").as("end"), first($"intervalLength").as("intervalLength"),
      sum($"end" - $"start").cast(IntegerType).as("coverageLength")).
      filter($"intervalLength" === $"coverageLength")
  }

  def loadMappabilityTrack(mappabilityPath: Path, mappabilityThreshold: Double): Dataset[SimpleInterval] = {
    def mappabilityIntervals = loadBigWig(mappabilityPath)

    logger.debug("Filtering mappability data")
    def selectedIntervals = mappabilityIntervals.filter(a => a.score >= mappabilityThreshold)
    logger.debug("Finished filtering mappability")

    logger.debug("Merging mappability intervals")
    def mergedIntervals = resultWriter.mergeIntervals(selectedIntervals.iterator).map {
      case interval: SimpleInterval => interval
      case interval: ContigInterval => SimpleInterval(interval.contigName, interval.start, interval.end)
    }.toSeq
    logger.debug("Finished merging mappability intervals")

    logger.debug("Distributing mappability data across the cluster")
    val mappabilityDS = sequila.sparkContext.parallelize(mergedIntervals).toDS.cache()
    logDiagnosticInfo(mappabilityDS, "mappability")
    mappabilityDS
  }

  def loadBigWig(bigWigPath: Path): Iterable[ScoredInterval] = {
    logger.debug("Loading mappability bigWig file")
    val bigWigFile = BigWigFile.read(bigWigPath, null)
    logger.debug("Finished loading mappability file")
    val sections = bigWigFile.getChromosomes.values.par.flatMap(chr => bigWigFile.query(chr.toString).asScala.flatMap(section =>
      section.query.iterator().asScala.map(iv => {
        new ScoredInterval(withChrPrefix(section.getChrom), iv.getStart, iv.getEnd, iv.getScore)
      })
    )).seq
    sections
  }

  def loadBaits(baitsLocation: String): Dataset[SimpleInterval] = {
    logger.debug("Loading baits data")
    val baitsDS = loadBed(baitsLocation).cache()
    logDiagnosticInfo(baitsDS, "baits")
    baitsDS
  }

  private def loadBed(bedLocation: String): Dataset[SimpleInterval] = {
    logger.debug("Loading bed file")
    val bedSchema = StructType(Array(
      StructField("contigName", StringType, nullable = false), StructField("start", IntegerType, nullable = false),
      StructField("end", IntegerType, nullable = false)))

    sequila.read.option("sep", "\t").schema(bedSchema).csv(bedLocation).as[SimpleInterval]
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
}
