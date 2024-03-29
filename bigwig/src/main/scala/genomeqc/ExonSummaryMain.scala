package genomeqc

import java.nio.file.Paths

import genomeqc.model.{SimpleInterval, StrandedInterval}
import org.apache.spark.sql.functions.sum
import org.apache.spark.storage.StorageLevel

object ExonSummaryMain {
  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val coverageThreshold = args(1).toInt
    val gtfLocation = args(2)
    val outputDirectory = Paths.get(args(3))

    val processor = new IntervalProcessor(outputDirectory)
    import processor.sequila.implicits._

    val highCoverageDS = processor.prepareJoinedHighCoverageDS(bamLocation, coverageThreshold)

    val exonDS = processor.prepareExonDF(gtfLocation).
      withColumn("intervalLength", $"end" - $"start").as[SimpleInterval]
    val exonCoverageIntersectionDF = processor.rangeJoin(highCoverageDS, exonDS).persist(StorageLevel.DISK_ONLY)
    val entirelyHighCoveredExons = processor.filterEntirelyCoveredIntervals(exonCoverageIntersectionDF,
      "exonId", "contigName", "strand").as[StrandedInterval]
    processor.writeCoverageStrandedRegions(entirelyHighCoveredExons,
      (strand: String) => outputDirectory.resolve(s"entirely_covered_exons_$strand"))

    val exonLengthDF = processor.prepareExonLengthDF(exonDS)

    val exonCoverageRatios = exonCoverageIntersectionDF.groupBy("transcriptId", "exonId", "contigName", "strand").agg(
      sum($"end" - $"start").as("exonCoverageLength")).join(exonLengthDF, "exonId")

    processor.resultWriter.writeExonSummary(exonCoverageRatios, outputDirectory.resolve("exon_summary.csv"))
  }
}
