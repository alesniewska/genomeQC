package genomeqc

import java.nio.file.Paths

import genomeqc.model.SimpleInterval
import org.apache.log4j.{LogManager, PropertyConfigurator}
import org.apache.spark.sql.Column

object CoverageMain {

  // use custom name, so that ugly $ suffix is not displayed
  private lazy val logger = LogManager.getLogger("CoverageMain")

  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(getClass.getClassLoader.getResource("log4j.properties"))
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

    logger.debug("Computing intersection of low coverage regions with low mappability regions")
    val regionsWithMappabilityDS = processor.rangeJoin(lowCoverageDS, mappabilityDS).as[SimpleInterval]
    processor.writeCoverageRegions(regionsWithMappabilityDS, "intersection_mappability")
    mappabilityDS.unpersist()
    val geneDS = processor.prepareGeneDS(gtfLocation)
    val lowCoverageGenes = processor.filterGenesWithLowCoverage(lowCoverageDS, geneDS, lowCoverageRatioThreshold)
    processor.writeLowCoveredGenes(lowCoverageGenes)
    logger.debug("Finished processing")
  }
}
