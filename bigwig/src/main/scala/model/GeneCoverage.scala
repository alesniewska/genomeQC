package model

case class GeneCoverage(strand: String, geneId: String, contigName: String, geneLength: Int, lowCoverageLength: Int, coverageList: Seq[SimpleInterval]) {
}
