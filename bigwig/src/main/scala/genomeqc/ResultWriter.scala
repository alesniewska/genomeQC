package genomeqc

import java.nio.file.Path

import genomeqc.model.{SimpleInterval, StrandedInterval}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Dataset}

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
    val coverageOnSinglePartition = coverageDS.orderBy("contigName", "start").coalesce(1)
    val dfWriter = processor.mergeIntervalsOnPartition(coverageOnSinglePartition).write.option("sep", "\t")
    writeCsvToSingleFile(dfWriter, outputPath)
  }

  def writeGeneSummary(geneList: DataFrame, outputPath: Path): Unit = {
    val dfWriter = geneList.select($"contigName".as("chromosome"), $"strand", $"geneId".as("gene"),
      ($"lowCoverageLength" / $"geneLength").as("gene_coverage")).coalesce(1).
      write.option("sep", ",").option("header", value = true)
    writeCsvToSingleFile(dfWriter, outputPath)
  }

  def writeExonSummary(geneList: DataFrame, outputPath: Path): Unit = {
    val dfWriter = geneList.select($"contigName".as("chromosome"), $"strand", $"geneId".as("gene"), $"exonId".as("exon"),
      $"transcriptId".as("transcript"), ($"exonCoverageLength" / $"exonLengthSum").as("exon_coverage")).
      coalesce(1).write.option("sep", ",").option("header", value = true)
    writeCsvToSingleFile(dfWriter, outputPath)
  }

  private def writeCsvToSingleFile[T](dfWriter: DataFrameWriter[T], outputPath: Path): Unit = {
    val tempDir = outputPath.getParent.resolve(outputPath.getFileName + ".tmp")
    dfWriter.csv(tempDir.toString)
    val partName = tempDir.toFile.list.filter(name => name.startsWith("part") && name.endsWith(".csv")).head
    val partPath = tempDir.resolve(partName)
    partPath.toFile.renameTo(outputPath.toFile)
    FileUtils.deleteDirectory(tempDir.toFile)
  }
}
