import java.nio.file.Paths

import IntervalProcessor._

object CoverageMain {
  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val gtfLocation = args(1)
    val coverageThreshold = args(2).toInt
    val lowCoverageRatioThreshold = args(3).toDouble
    val outputDirectory = Paths.get(args(4))
    val chromosomeLengthPath = Paths.get(args(5))

    chromosomeLengthMap = readChromosomeLengths(chromosomeLengthPath)

    outputDirectory.toFile.mkdirs()
    val coverageDS = prepareCoverageDS(bamLocation, coverageThreshold).cache
    writeLowCoveredRegions(coverageDS, outputDirectory)
    val geneDS = prepareGeneDS(gtfLocation)
    val lowCoveredGeneDS = filterGenesWithLowCoverage(coverageDS, geneDS, lowCoverageRatioThreshold)
    val lowCoveredGenesByStrandChromosome = lowCoveredGeneDS.rdd.groupBy(row => (row.strand, row.chromosome)).collect
    writePartiallyLowCoveredGenes(lowCoveredGenesByStrandChromosome, outputDirectory)
    writeEntirelyLowCoveredGenes(lowCoveredGenesByStrandChromosome, outputDirectory)
  }
}
