package model

case class GeneCoverage(geneId: String, chromosome: String, geneLength: Int, lowCoverageLength: Int, coverageList: Seq[IntervalCoverage]) {
}
