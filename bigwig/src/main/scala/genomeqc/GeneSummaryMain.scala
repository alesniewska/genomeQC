package genomeqc

import java.nio.file.Paths

object GeneSummaryMain {
  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val coverageThreshold = args(1).toInt
    val gtfLocation = args(2)
    val outputDirectory = Paths.get(args(3))

    val processor = new IntervalProcessor(outputDirectory)

    val lowCoverageCommonDS = processor.prepareJoinedLowCoverageDS(bamLocation, coverageThreshold)
    val geneDS = processor.prepareGeneDS(gtfLocation)
    val lowCoveredGeneDS = processor.filterGenesWithLowCoverage(lowCoverageCommonDS, geneDS, 0.0)
    processor.writeGeneSummary(lowCoveredGeneDS)
  }
}
