import java.nio.file.Paths

import model._
import org.apache.spark.sql.Column

object CoverageMain {
  def main(args: Array[String]): Unit = {
    val bamLocation = args(0)
    val gtfLocation = args(1)
    val coverageThreshold = args(2).toInt
    val lowCoverageRatioThreshold = args(3).toDouble
    val outputDirectory = Paths.get(args(4))
    val mappabilityPath = Paths.get(args(5))
    val mappabilityThreshold = args(6).toDouble

    val processor = new IntervalProcessor(outputDirectory)
    outputDirectory.toFile.mkdirs()
    import processor.sequila.implicits._

    val coverageFilter = (coverageColumn: Column) => coverageColumn < coverageThreshold
    val lowCoverageDS = processor.writeRegionsAndIntersection(bamLocation, coverageFilter)
    val mappabilityDS = processor.loadMappabilityTrack(mappabilityPath, mappabilityThreshold)
    val regionsWithMappabilityDS = processor.rangeJoin(lowCoverageDS, mappabilityDS).as[SimpleInterval]
    processor.writeCoverageRegions(regionsWithMappabilityDS, "intersection_mappability")
    val geneDS = processor.prepareGeneDS(gtfLocation)
    val lowCoveredGeneDS = processor.filterGenesWithLowCoverage(lowCoverageDS, geneDS, lowCoverageRatioThreshold)
    val lowCoveredGenesByStrandChromosome = lowCoveredGeneDS.rdd.groupBy(row => (row.strand, row.chromosome)).collect
    processor.resultWriter.writeGeneSummary(lowCoveredGenesByStrandChromosome.flatMap(_._2), outputDirectory.resolve("gene_summary.txt"))
    processor.writePartiallyLowCoveredGenes(lowCoveredGenesByStrandChromosome)
    processor.writeEntirelyLowCoveredGenes(lowCoveredGenesByStrandChromosome)
  }
}
