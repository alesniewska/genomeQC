import java.nio.file.Path

import gnu.trove.list.array.{TFloatArrayList, TIntArrayList}
import model.{ContigInterval, SimpleInterval}
import org.jetbrains.bio.CompressionType
import org.jetbrains.bio.big.{BedGraphSection, BigWigFile}

import scala.collection.JavaConverters._
import scala.io.Source

class BigWigWriter(chromosomeLengthPath: Path) {

  private val chromosomeLengthMap = readChromosomeLengths(chromosomeLengthPath)

  /**
    *
    * @param outputPathBuilder takes strand keyword and produces output path.
    */
  def writeTwoBigWigFiles(lowCoveredGenesByStrandChromosome: Iterable[((String, String), Iterable[_ <: ContigInterval])], outputPathBuilder: String => Path): Unit = {
    val (coverageOnPlus, coverageOnMinus) = lowCoveredGenesByStrandChromosome.partition(_._1._1 == "+")
    val removeStrand = (covWithStrand: Iterable[((String, String), Iterable[_ <: ContigInterval])]) => covWithStrand.map(p => p._1._2 -> p._2)
    writeToBigWig(removeStrand(coverageOnMinus), outputPathBuilder("neg"))
    writeToBigWig(removeStrand(coverageOnPlus), outputPathBuilder("pos"))
  }

  def writeToBigWig(coverageByChromosome: Iterable[(String, Iterable[_ <: ContigInterval])], outputPath: Path): Unit = {
    val mergedCoverageIntervalsByChr = coverageByChromosome.map(p => p._1 -> mergeIntervals(p._2))
    val sectionList = mergedCoverageIntervalsByChr.map({ chrIntervals =>
      val chromosomeName = chrIntervals._1
      val intervals = chrIntervals._2
      val startList = intervals.map(_.start).toArray
      val endList = intervals.map(_.end + 1).toArray
      val coverageList = intervals.map(_ => 1f).toArray
      new BedGraphSection(chromosomeName, TIntArrayList.wrap(startList), TIntArrayList.wrap(endList), TFloatArrayList.wrap(coverageList))
    }).toList.asJava
    val chromosomeLengthArray = chromosomeLengthMap.toSeq.map(p => new kotlin.Pair[String, Integer](p._1, p._2)).asJava
    BigWigFile.write(sectionList, chromosomeLengthArray, outputPath, 1, CompressionType.DEFLATE)
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

  def readChromosomeLengths(chrFilePath: Path): Map[String, Int] = {
    val src = Source.fromFile(chrFilePath.toFile)
    try {
      src.getLines.map(_.split("\\s+")).map(p => p(0) -> p(1).toInt).toMap
    } finally {
      src.close()
    }
  }
}
