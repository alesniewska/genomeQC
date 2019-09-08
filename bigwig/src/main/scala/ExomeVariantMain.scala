import java.nio.file.Paths

import model.{SimpleInterval, StrandedInterval}
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.sum

object ExomeVariantMain {
  def main(args: Array[String]): Unit = {
    PropertyConfigurator.configure(getClass.getClassLoader.getResource("log4j.properties"))
    val bamLocation = args(0)
    val baitBedLocation = args(1)
    val gtfLocation = args(2)
    val coverageThreshold = args(3).toInt
    val outputDirectory = Paths.get(args(4))

    val processor = new IntervalProcessor(outputDirectory)
    outputDirectory.toFile.mkdirs()
    import processor.sequila.implicits._

    val coverageFilter = (coverageColumn: Column) => coverageColumn > coverageThreshold
    val highCoverageDS = processor.writeRegionsAndIntersection(bamLocation, coverageFilter)

    val baitsDS = processor.loadBaits(baitBedLocation).
      withColumn("intervalLength", $"end" - $"start").as[SimpleInterval]
    val baitCoverageIntersectionDS = processor.rangeJoin(highCoverageDS, baitsDS).cache.as[SimpleInterval]
    processor.writeCoverageRegions(baitCoverageIntersectionDS, "baits")
    baitsDS.unpersist()
    val entirelyHighCoveredBaits = processor.filterEntirelyCoveredIntervals(baitCoverageIntersectionDS,
      "contigName").as[SimpleInterval]
    processor.writeCoverageRegions(entirelyHighCoveredBaits, "entirely_covered_baits")
    baitCoverageIntersectionDS.unpersist()

    // TODO: Load GTF only once
    val geneDS = processor.prepareGeneDS(gtfLocation).
      withColumn("intervalLength", $"end" - $"start").as[StrandedInterval]
    val geneCoverageIntersectionDS = processor.rangeJoin(highCoverageDS, geneDS).cache.as[StrandedInterval]
    processor.writeCoverageStrandedRegions(geneCoverageIntersectionDS,
      (strand: String) => outputDirectory.resolve(s"genes_${strand}_high_coverage"))
    geneDS.unpersist()
    val entirelyHighCoveredGenes = processor.filterEntirelyCoveredIntervals(geneCoverageIntersectionDS,
      "geneId", "contigName", "strand").as[StrandedInterval]
    processor.writeCoverageStrandedRegions(entirelyHighCoveredGenes,
      (strand: String) => outputDirectory.resolve(s"entirely_covered_genes_$strand"))
    geneCoverageIntersectionDS.unpersist()

    val exonDS = processor.prepareExonDS(gtfLocation).
      withColumn("intervalLength", $"end" - $"start").as[SimpleInterval]
    val exonCoverageIntersectionDS = processor.rangeJoin(highCoverageDS, exonDS).cache.as[StrandedInterval]
    processor.writeCoverageStrandedRegions(exonCoverageIntersectionDS,
      (strand: String) => outputDirectory.resolve(s"exons_${strand}_high_coverage"))
    val entirelyHighCoveredExons = processor.filterEntirelyCoveredIntervals(exonCoverageIntersectionDS,
      "exonId", "contigName", "strand").as[StrandedInterval]
    processor.writeCoverageStrandedRegions(entirelyHighCoveredExons,
      (strand: String) => outputDirectory.resolve(s"entirely_covered_exons_$strand"))

    val geneLengthDF = processor.prepareGeneLengthDF(exonDS)


    val exonCoverageRatios = exonCoverageIntersectionDS.groupBy("geneId", "contigName", "strand").agg(
        sum($"end" - $"start").as("exonCoverageLength")).join(geneLengthDF, "geneId")

    processor.resultWriter.writeGeneExonSummary(exonCoverageRatios, outputDirectory.resolve("exon_gene_summary.txt"))

  }
}
