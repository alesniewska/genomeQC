import java.nio.file.Paths

import model._

object CoverageMain {
  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val gtfLocation = args(1)
    val coverageThreshold = args(2).toInt
    val lowCoverageRatioThreshold = args(3).toDouble
    val outputDirectory = Paths.get(args(4))
    val chromosomeLengthPath = Paths.get(args(5))

    val processor = new IntervalProcessor(chromosomeLengthPath)

    import processor.sequila.implicits._

    outputDirectory.toFile.mkdirs()
    val sampleDSList = processor.prepareLowCoverageSamples(bamLocation, coverageThreshold)
      .map(p => (p._1.cache.as[SimpleInterval], p._2))
    sampleDSList.foreach(p => processor.writeLowCoveredRegions(p._1, p._2, outputDirectory))
    val lowCoverageDS = processor.simpleRangeJoinList(sampleDSList.map(_._1)).cache()
    lowCoverageDS.count()
    sampleDSList.foreach(_._1.unpersist)
    val geneDS = processor.prepareGeneDS(gtfLocation)
    val lowCoveredGeneDS = processor.filterGenesWithLowCoverage(lowCoverageDS, geneDS, lowCoverageRatioThreshold)
    val lowCoveredGenesByStrandChromosome = lowCoveredGeneDS.rdd.groupBy(row => (row.strand, row.chromosome)).collect
    processor.writePartiallyLowCoveredGenes(lowCoveredGenesByStrandChromosome, outputDirectory)
    processor.writeEntirelyLowCoveredGenes(lowCoveredGenesByStrandChromosome, outputDirectory)
  }
}
