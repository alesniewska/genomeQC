package genomeqc

import java.nio.file.Path

import genomeqc.model.{SimpleInterval, StrandedInterval}
import org.apache.spark.sql.{DataFrame, Dataset}

class ResultWriter(processor: IntervalProcessor) {

  import processor.sequila.implicits._

  /**
    *
    * @param outputPathBuilder takes strand keyword and produces output path.
    */
  def writePairOfResults(coverageDS: Dataset[StrandedInterval], outputPathBuilder: String => Path): Unit = {
    coverageDS.cache()
    val coverageOnPlus = coverageDS.filter($"strand" === "+")
    val coverageOnMinus = coverageDS.filter($"strand" === "-")
    writeResultToBedFile(coverageOnPlus.drop("strand").as[SimpleInterval], outputPathBuilder("neg"))
    writeResultToBedFile(coverageOnMinus.drop("strand").as[SimpleInterval], outputPathBuilder("pos"))
    coverageDS.unpersist()
  }

  def writeResultToBedFile(coverageDS: Dataset[SimpleInterval], outputPath: Path): Unit = {
    val tempPath = outputPath.resolve("temp")
    val coverageOnSinglePartition = coverageDS.orderBy("contigName", "start").coalesce(1)
    processor.mergeIntervalsOnPartition(coverageOnSinglePartition).write.option("sep", "\t").csv(tempPath.toString)
  }

  def writeGeneSummary(geneList: DataFrame, outputPath: Path): Unit = {
    geneList.select($"contigName".as("chromosome"), $"strand", $"geneId".as("gene"),
      ($"lowCoverageLength" / $"geneLength").as("gene_coverage")).coalesce(1).
      write.option("sep", ",").option("header", value = true).csv(outputPath.toString)
  }

  def writeExonSummary(geneList: DataFrame, outputPath: Path): Unit = {
    geneList.select($"contigName".as("chromosome"), $"strand", $"geneId".as("gene"), $"exonId".as("exon"),
      $"transcriptId".as("transcript"), ($"exonCoverageLength" / $"exonLengthSum").as("exon_coverage")).
      coalesce(1).write.option("sep", ",").option("header", value = true).csv(outputPath.toString)
  }
}
