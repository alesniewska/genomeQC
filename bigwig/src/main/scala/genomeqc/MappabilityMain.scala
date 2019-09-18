package genomeqc

import java.nio.file.Paths

import genomeqc.model.SimpleInterval

object MappabilityMain {
  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val coverageThreshold = args(1).toInt
    val mappabilityPath = Paths.get(args(2))
    val mappabilityThreshold = args(3).toDouble
    val outputDirectory = Paths.get(args(4))

    val processor = new IntervalProcessor(outputDirectory)
    import processor.sequila.implicits._

    val lowCoverageCommonDS = processor.prepareJoinedLowCoverageDS(bamLocation, coverageThreshold)
    val mappabilityDS = processor.loadMappabilityTrack(mappabilityPath, mappabilityThreshold)
    val coverageMappabilityDS = processor.rangeJoin(lowCoverageCommonDS, mappabilityDS).as[SimpleInterval]
    processor.writeCoverageRegions("mappability", coverageMappabilityDS)
  }
}
