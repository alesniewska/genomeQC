package genomeqc

import java.nio.file.Path

import genomeqc.model.{ContigInterval, SimpleInterval, StrandedInterval}
import org.apache.spark.sql.{DataFrame, Dataset, SequilaSession}

class ResultWriter(sequila: SequilaSession) {

  import sequila.implicits._

  val mergeIntervals: Iterator[_ <: ContigInterval] => Iterator[SimpleInterval] = (intervals: Iterator[_ <: ContigInterval]) => {
    intervals.foldLeft(List[SimpleInterval]())((acc, interval) => {
      val previousInterval = acc.headOption
      if (previousInterval.isDefined && previousInterval.get.end + 1 == interval.start) {
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
    val mergeIntervalsBC = sequila.sparkContext.broadcast(mergeIntervals)
    coverageDS.orderBy("contigName", "start").coalesce(1).
      mapPartitions(it => mergeIntervalsBC.value(it)).
      write.option("sep", "\t").csv(outputPath.toString)
  }

  def writeGeneSummary(geneList: DataFrame, outputPath: Path): Unit = {
    geneList.select($"contigName".as("chromosome"), $"strand", $"geneId".as("gene"),
      ($"lowCoverageLength" / $"geneLength").as("gene_coverage")).coalesce(1).
      write.option("sep", ",").option("header", value = true).csv(outputPath.toString)
  }

  def writeGeneExonSummary(geneList: DataFrame, outputPath: Path): Unit = {
    geneList.select($"contigName".as("chromosome"), $"strand", $"geneId".as("gene"),
      ($"exonCoverageLength" / $"exonLengthSum").as("exon_coverage")).coalesce(1).
      write.option("sep", ",").option("header", value = true).csv(outputPath.toString)
  }
}
