import java.nio.file.Path

import IntervalProcessor.chromosomeLengthMap
import gnu.trove.list.array.{TFloatArrayList, TIntArrayList}
import model.IntervalCoverage
import org.jetbrains.bio.CompressionType
import org.jetbrains.bio.big.{BedGraphSection, BigWigFile}

import scala.collection.JavaConverters._

object BigWigWriter {
  /**
    *
    * @param outputPathBuilder takes strand keyword and produces output path.
    */
  def writeTwoBigWigFiles(lowCoveredGenesByStrandChromosome: Iterable[((String, String), Iterable[IntervalCoverage])], outputPathBuilder: String => Path): Unit = {
    val (coverageOnPlus, coverageOnMinus) = lowCoveredGenesByStrandChromosome.partition(_._1._1 == "+")
    val removeStrand = (covWithStrand: Iterable[((String, String), Iterable[IntervalCoverage])]) => covWithStrand.map(p => p._1._2 -> p._2)
    writeToBigWig(removeStrand(coverageOnMinus), outputPathBuilder("neg"))
    writeToBigWig(removeStrand(coverageOnPlus), outputPathBuilder("pos"))
  }

  def writeToBigWig(coverageByChromosome: Iterable[(String, Iterable[IntervalCoverage])], outputPath: Path): Unit = {
    val sectionList = coverageByChromosome.map({ chrIntervals =>
      val chromosomeName = chrIntervals._1
      val intervals = chrIntervals._2
      val startList = intervals.map(_.start).toArray
      val endList = intervals.map(_.end + 1).toArray
      val coverageList = intervals.map(_.coverage.toFloat).toArray
      new BedGraphSection(chromosomeName, TIntArrayList.wrap(startList), TIntArrayList.wrap(endList), TFloatArrayList.wrap(coverageList))
    }).toList.asJava
    val chromosomeLengthArray = chromosomeLengthMap.toList.map(p => new kotlin.Pair[String, Integer](p._1, p._2)).asJava
    BigWigFile.write(sectionList, chromosomeLengthArray, outputPath, 8, CompressionType.DEFLATE)
  }

}
