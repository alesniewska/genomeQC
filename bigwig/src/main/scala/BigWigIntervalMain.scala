import java.nio.file.Paths

import IntervalProcessor.{chromosomeLengthMap, readChromosomeLengths}

object BigWigIntervalMain {
  def main(args: Array[String]): Unit = {
    val coveragePath = Paths.get(args(0))
    val mappabilityPath = Paths.get(args(1))
    val chromosomeLengthPath = Paths.get(args(2))
    val outputPath = Paths.get(args(3))

    chromosomeLengthMap = readChromosomeLengths(chromosomeLengthPath)


    val coverageIntersectionDS = IntervalProcessor.intersectBigWigs(coveragePath, mappabilityPath)
    val coverageByChromosome = IntervalProcessor.collectByChromosome(coverageIntersectionDS)
    BigWigWriter.writeToBigWig(coverageByChromosome, outputPath)
  }
}
