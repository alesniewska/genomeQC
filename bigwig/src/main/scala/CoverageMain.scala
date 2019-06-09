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
    val mappabilityPath = Paths.get(args(6))
    val mappabilityThreshold = 1.0

    val processor = new IntervalProcessor(chromosomeLengthPath, outputDirectory)

    import processor.sequila.implicits._

    processor.loadBigWig(mappabilityPath)

    outputDirectory.toFile.mkdirs()
    val sampleDSList = processor.prepareLowCoverageSamples(bamLocation, coverageThreshold)
      .map(p => (p._1.cache.as[SimpleInterval], p._2))
    sampleDSList.foreach(p => processor.writeLowCoveredRegions(p._1, p._2))
    val lowCoverageDS = processor.simpleRangeJoinList(sampleDSList.map(_._1)).cache
    processor.writeLowCoveredRegions(lowCoverageDS, "intersection")
    sampleDSList.foreach(_._1.unpersist)
    val mappabilityDS = processor.loadMappabilityTrack(mappabilityPath, mappabilityThreshold)
    val regionsWithMappabilityDS = processor.rangeJoin(lowCoverageDS, mappabilityDS).as[SimpleInterval]
    processor.writeLowCoveredRegions(regionsWithMappabilityDS, "intersection_mappability")
    val geneDS = processor.prepareGeneDS(gtfLocation)
    val lowCoveredGeneDS = processor.filterGenesWithLowCoverage(lowCoverageDS, geneDS, lowCoverageRatioThreshold)
    val lowCoveredGenesByStrandChromosome = lowCoveredGeneDS.rdd.groupBy(row => (row.strand, row.chromosome)).collect
    processor.writeGeneSummary(lowCoveredGenesByStrandChromosome.flatMap(_._2))
    processor.writePartiallyLowCoveredGenes(lowCoveredGenesByStrandChromosome)
    processor.writeEntirelyLowCoveredGenes(lowCoveredGenesByStrandChromosome)
  }
}
