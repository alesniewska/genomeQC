package genomeqc

import java.nio.file.Paths

import genomeqc.model.SimpleInterval
import org.apache.spark.storage.StorageLevel

object HighCoveredBaitsMain {
  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val coverageThreshold = args(1).toInt
    val baitBedLocation = args(2)
    val outputDirectory = Paths.get(args(3))

    val processor = new IntervalProcessor(outputDirectory)
    import processor.sequila.implicits._

    val highCoverageDS = processor.prepareJoinedHighCoverageDS(bamLocation, coverageThreshold)

    val baitsDS = processor.loadBaits(baitBedLocation).
      withColumn("intervalLength", $"end" - $"start").
      withColumn("baitStart", $"start").withColumn("baitEnd", $"end").as[SimpleInterval]
    val baitCoverageIntersectionDF = processor.rangeJoin(highCoverageDS, baitsDS).persist(StorageLevel.DISK_ONLY)
    processor.writeCoverageRegions("high_covered_baits", baitCoverageIntersectionDF.as[SimpleInterval])
    val entirelyHighCoveredBaits = processor.filterEntirelyCoveredIntervals(baitCoverageIntersectionDF,
      "contigName", "baitStart", "baitEnd").select($"baitStart".as("start"),
      $"baitEnd".as("end"), $"contigName").as[SimpleInterval]
    processor.writeCoverageRegions("entirely_covered_baits", entirelyHighCoveredBaits)
  }
}
