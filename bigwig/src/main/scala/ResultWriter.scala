import java.io.PrintWriter
import java.nio.file.Path

import model.{ContigInterval, GeneCoverage, SimpleInterval}

class ResultWriter {

  /**
    *
    * @param outputPathBuilder takes strand keyword and produces output path.
    */
  def writePairOfResults(lowCoveredGenesByStrandChromosome: Iterable[((String, String), Iterable[_ <: ContigInterval])], outputPathBuilder: String => Path): Unit = {
    val (coverageOnPlus, coverageOnMinus) = lowCoveredGenesByStrandChromosome.partition(_._1._1 == "+")
    val removeStrand = (covWithStrand: Iterable[((String, String), Iterable[_ <: ContigInterval])]) => covWithStrand.map(p => p._1._2 -> p._2)
    writeResultToBedFile(removeStrand(coverageOnMinus), outputPathBuilder("neg"))
    writeResultToBedFile(removeStrand(coverageOnPlus), outputPathBuilder("pos"))
  }

  def writeResultToBedFile(coverageByChromosome: Iterable[(String, Iterable[_ <: ContigInterval])], outputPath: Path): Unit = {
    val mergedCoverageIntervals = coverageByChromosome.map(p => p._1 -> mergeIntervals(p._2)).flatMap(_._2)
    val intervalConsumer = (interval: ContigInterval) => "%s\t%s\t%s".format(interval.contigName, interval.start, interval.end)
    writeToFile(mergedCoverageIntervals, intervalConsumer, None, outputPath)
  }

  def mergeIntervals(intervals: Iterable[_ <: ContigInterval]): Iterable[_ <: ContigInterval] = {
    intervals.foldLeft(List[ContigInterval]())((acc, interval) => {
      val previousInterval = acc.headOption
      if (previousInterval.isDefined && previousInterval.get.end + 1 == interval.start) {
        SimpleInterval(interval.contigName, previousInterval.get.start, interval.end) :: acc.tail
      } else {
        interval :: acc
      }
    }).reverse
  }

  def writeGeneSummary(geneList: Iterable[GeneCoverage], outputPath: Path): Unit = {
    val calcGeneCovRatio = (gene: GeneCoverage)=> 1.0 * gene.lowCoverageLength / gene.geneLength
    val geneConsumer = (gene: GeneCoverage) => "%s,%s,%s,%.2f".format(gene.chromosome, gene.strand, gene.geneId, calcGeneCovRatio(gene))
    val header = Some("chromosome,strand,gene,gene_coverage")
    writeToFile(geneList, geneConsumer, header, outputPath)
  }

  def writeGeneExonSummary(geneList: Iterable[(String, String, String, Double)], outputPath: Path): Unit = {
    val geneConsumer = (gene: (String, String, String, Double)) => "%s,%s,%s,%.2f".format(gene._1, gene._2, gene._3, gene._4)
    val header = Some("chromosome,strand,gene,exon_coverage")
    writeToFile(geneList, geneConsumer, header, outputPath)
  }

  private def writeToFile[A](records: Iterable[A], recordConsumer: A => String, header: Option[String], outputPath: Path): Unit = {
    val writer = new PrintWriter(outputPath.toFile)
    try {
      if (header.isDefined) {
        writer.println(header.get)
      }
      records.foreach(record => {
        val line = recordConsumer(record)
        writer.println(line)
      })
    } finally {
      writer.close()
    }
  }
}
