package genomeqc

import java.nio.file.Paths

object LowCoveredGenesMain {
  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val coverageThreshold = args(1).toInt
    val gtfLocation = args(2)
    val lowCoverageRatioThreshold = args(3).toDouble
    val outputDirectory = Paths.get(args(4))

    val processor = new IntervalProcessor(outputDirectory)

    val lowCoverageCommonDS = processor.prepareJoinedLowCoverageDS(bamLocation, coverageThreshold)
    val geneDS = processor.prepareGeneDS(gtfLocation)
    val lowCoveredGeneDS = processor.filterGenesWithLowCoverage(lowCoverageCommonDS, geneDS, lowCoverageRatioThreshold)
    processor.writeLowCoveredGenes(lowCoveredGeneDS)
  }
}
