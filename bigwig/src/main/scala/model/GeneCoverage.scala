package model

case class GeneCoverage(strand: String, geneId: String, chromosome: String, geneLength: Int, lowCoverageLength: Int, coverageList: Seq[SimpleInterval]) {
}
