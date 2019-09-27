package genomeqc

import java.nio.file.Path

import genomeqc.model._
import org.apache.log4j.{LogManager, PropertyConfigurator}
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
  val resultWriter = new ResultWriter(this)

  import sequila.implicits._

  outputDirectory.toFile.mkdirs()

  def configureSequilaSession: SequilaSession = {
    PropertyConfigurator.configure(getClass.getClassLoader.getResource("log4j.properties"))
    logger.debug("Initializing Sequila")
    val spark = SparkSession.builder()
      .getOrCreate()
    val ss = SequilaSession(spark)
    SequilaRegister.register(ss)
    UDFRegister.register(ss)
    logger.debug("Finished initializing Sequila")
    ss
  }

  def prepareLowCoverageSamples(bamLocation: String, coverageThreshold: Int): List[(String, Dataset[SimpleInterval])] = {
    logger.debug("Loading BAM files")
    prepareCoverageSamples(bamLocation).map(p =>
      p._1 -> mergeIntervalsOnPartition(p._2.filter($"coverage" < coverageThreshold).as[SimpleInterval])
    )
  }

  def prepareHighCoverageSamples(bamLocation: String, coverageThreshold: Int): List[(String, Dataset[SimpleInterval])] = {
    logger.debug("Loading BAM files")
    prepareCoverageSamples(bamLocation).map(p =>
      p._1 -> mergeIntervalsOnPartition(p._2.filter($"coverage" > coverageThreshold).as[SimpleInterval])
    )
  }

  def prepareJoinedHighCoverageDS(bamLocation: String, coverageThreshold: Int): Dataset[SimpleInterval] = {
    val sampleDSList = prepareHighCoverageSamples(bamLocation, coverageThreshold)
    joinCoverageSamples(sampleDSList.map(_._2))
  }

  def prepareJoinedLowCoverageDS(bamLocation: String, coverageThreshold: Int): Dataset[SimpleInterval] = {
    val sampleDSList = prepareLowCoverageSamples(bamLocation, coverageThreshold)
    joinCoverageSamples(sampleDSList.map(_._2))
  }

  def joinCoverageSamples(sampleDSList: List[Dataset[SimpleInterval]]): Dataset[SimpleInterval] = {
    val sampleCount = sampleDSList.size
    if (sampleCount > 1) {
      logger.info(s"Computing intersection of low coverage interval across $sampleCount samples")
      val lowCoverageDS = simpleRangeJoinList(sampleDSList)
      lowCoverageDS
    } else {
      logger.info("Only one bam file provided - skipping computing intersection")
      sampleDSList.head
    }
  }

  def mergeIntervals(intervals: Iterator[_ <: ContigInterval]): Iterator[SimpleInterval] = {
    intervals.foldLeft(List[SimpleInterval]())((acc, interval) => {
      val previousInterval = acc.headOption
      if (previousInterval.isDefined && previousInterval.get.contigName == interval.contigName &&
        previousInterval.get.end + 1 == interval.start) {
        SimpleInterval(interval.contigName, previousInterval.get.start, interval.end) :: acc.tail
      } else {
        val simpleInterval = interval match {
          case SimpleInterval(_, _, _) => interval.asInstanceOf[SimpleInterval]
          case other => SimpleInterval(other.contigName, other.start, other.end)
        }
        simpleInterval :: acc
      }
    }).reverseIterator
  }

  def mergeIntervalsOnPartition(intervalDS: Dataset[SimpleInterval]): Dataset[SimpleInterval] = {
    // TODO: DRY
    intervalDS.mapPartitions(part => {
      part.foldLeft(List[SimpleInterval]())((acc, interval) => {
        val previousInterval = acc.headOption
        if (previousInterval.isDefined && previousInterval.get.contigName == interval.contigName &&
          previousInterval.get.end + 1 == interval.start) {
          SimpleInterval(interval.contigName, previousInterval.get.start, interval.end) :: acc.tail
        } else {
          interval :: acc
        }
      }).reverseIterator
    })
  }

  def writeCoverageRegions(sampleName: String, coverageDS: Dataset[SimpleInterval]): Unit = {
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
      GeneInterval(i.contigName, i.start, i.end, gene.strand, gene.geneId, gene.lowCoverageLength, gene.geneLength)))

    val lowCoverageOutputPathBuilder = (strand: String) => outputDirectory.resolve(s"low_coverage_genes_$strand")

    logger.debug("Writing substandard genes")
    resultWriter.writePairOfResults(geneIntervalDF.as[StrandedInterval], lowCoverageOutputPathBuilder)
  }

  def writeGeneSummary(geneCoverageDS: Dataset[GeneCoverage]): Unit = {
    val geneSummaryPath = outputDirectory.resolve("gene_summary.csv")
    logger.debug("Writing gene summary")
    resultWriter.writeGeneSummary(geneCoverageDS.toDF, geneSummaryPath)
  }


  def prepareCoverageSamples(bamLocation: String): List[(String, Dataset[SimpleInterval])] = {
    sequila.sql(s"create database if not exists $DB_NAME")
    sequila.sql(s"use $DB_NAME")
    sequila.sql(s"drop table if exists $BAM_TABLE_NAME")
    sequila.sql(s"create table $BAM_TABLE_NAME USING org.biodatageeks.datasources.BAM.BAMDataSource OPTIONS(path '$bamLocation')")
    val sampleIdList = sequila.table(BAM_TABLE_NAME).select("sampleId").distinct().as[String].collect()
    sampleIdList.map(sampleName =>
      sampleName -> sequila.sql(s"select * from bdg_coverage('$BAM_TABLE_NAME','$sampleName', 'blocks')").
        select(withChrPrefix($"contigName").as("contigName"), $"start", $"end").as[SimpleInterval]
    ).toList
  }

  def prepareGeneDS(gtfLocation: String): Dataset[Gene] = {
    logger.debug("Loading gene data")
    val geneIdExpression = (attrsColumn: Column) =>
      regexp_extract(attrsColumn, """.*gene_id\s"([^"]+)";""", 1).as("geneId")
    loadGtf(gtfLocation, "gene", geneIdExpression).as[Gene]
  }

  def prepareExonDF(gtfLocation: String): DataFrame = {
    logger.debug("Loading exon data")
    val geneIdExpression = (attrsColumn: Column) =>
      regexp_extract(attrsColumn, """.*gene_id\s"([^"]+)";""", 1).as("geneId")
    val exonIdExpression = (attrsColumn: Column) =>
      regexp_extract(attrsColumn, """.*exon_id\s"([^"]+)";""", 1).as("exonId")
    val transcriptIdExpression = (attrsColumn: Column) =>
      regexp_extract(attrsColumn, """.*transcript_id\s"([^"]+)";""", 1).as("transcriptId")
    loadGtf(gtfLocation, "exon", geneIdExpression, exonIdExpression, transcriptIdExpression)
  }

  def loadGtf(gtfLocation: String, featureType: String, attributeExpressions: (Column => Column)*): DataFrame = {
    val gtfSchema = StructType(Array(
      StructField("seqname", StringType, nullable = false), StructField("source", StringType, nullable = false),
      StructField("feature", StringType, nullable = false), StructField("start", IntegerType, nullable = false),
      StructField("end", IntegerType, nullable = false), StructField("score", StringType, nullable = false),
      StructField("strand", StringType, nullable = false), StructField("frame", StringType, nullable = false),
      StructField("attributes", StringType, nullable = false)
    ))
    val resultColumns = Array(withChrPrefix($"seqname").as("contigName"),
      $"start", $"end", $"strand") ++ attributeExpressions.map(_.apply($"attributes"))
    val gtfDF = sequila.read.option("sep", "\t").option("comment", "#").schema(gtfSchema).csv(gtfLocation).
      select("seqname", "feature", "start", "end", "strand", "attributes").
      filter($"feature" === featureType).
      select(resultColumns: _*)
    gtfDF
  }

  def prepareExonLengthDF(exonDS: Dataset[SimpleInterval]): DataFrame = {
    val geneLengthDF = exonDS.groupBy($"exonId").agg(first($"geneId").as("geneId"),
      first($"end" - $"start").as("exonLengthSum"))
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

  def filterEntirelyCoveredIntervals(coverageIntervalsDF: DataFrame,
                                     intervalComplexId: String*): DataFrame = {
    logger.debug("Filtering entirely covered intervals")
    val aggKeyColumns = intervalComplexId.map(col)

    coverageIntervalsDF.groupBy(aggKeyColumns: _*).agg(
      first($"start").as("start"), first($"end").as("end"), first($"intervalLength").as("intervalLength"),
      sum($"end" - $"start").cast(IntegerType).as("coverageLength")).
      filter($"intervalLength" === $"coverageLength")
  }

  def loadMappabilityTrack(mappabilityPath: Path, mappabilityThreshold: Double): Dataset[SimpleInterval] = {
    def mappabilityIntervals = loadBigWig(mappabilityPath)

    logger.debug("Filtering mappability data")

    def selectedIntervals = mappabilityIntervals.filter(a => a.score < mappabilityThreshold)

    logger.debug("Finished filtering mappability")

    logger.debug("Merging mappability intervals")

    def mergedIntervals = mergeIntervals(selectedIntervals.iterator).map {
      case interval: SimpleInterval => interval
      case interval: ContigInterval => SimpleInterval(interval.contigName, interval.start, interval.end)
    }.toSeq

    logger.debug("Finished merging mappability intervals")

    logger.debug("Distributing mappability data across the cluster")
    val mappabilityDS = sequila.sparkContext.parallelize(mergedIntervals).toDS
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
    val baitsDS = loadBed(baitsLocation)
    baitsDS
  }

  private def loadBed(bedLocation: String): Dataset[SimpleInterval] = {
    logger.debug("Loading bed file")
    val bedSchema = StructType(Array(
      StructField("contigName", StringType, nullable = false), StructField("start", IntegerType, nullable = false),
      StructField("end", IntegerType, nullable = false)))
    sequila.read.option("sep", "\t").schema(bedSchema).csv(bedLocation).select($"start", $"end",
      withChrPrefix($"contigName").as("contigName")).as[SimpleInterval]
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
