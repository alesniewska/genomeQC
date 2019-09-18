package genomeqc

import java.nio.file.Paths

object CoverageMain {
  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val coverageThreshold = args(1).toInt
    val outputDirectory = Paths.get(args(2))

    val processor = new IntervalProcessor(outputDirectory)

    val lowCoverageSamplesDS = processor.prepareLowCoverageSamples(bamLocation, coverageThreshold)
    if (lowCoverageSamplesDS.size > 1) {
      lowCoverageSamplesDS.foreach(_._2.cache())
    }
    lowCoverageSamplesDS.foreach(sample => processor.writeCoverageRegions(sample._1, sample._2))
    if (lowCoverageSamplesDS.size > 1) {
      val lowCoverageCommonDS = processor.simpleRangeJoinList(lowCoverageSamplesDS.map(_._2))
      processor.writeCoverageRegions("intersection", lowCoverageCommonDS)
    }
  }
}
