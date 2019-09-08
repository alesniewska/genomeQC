package genomeqc.model

case class GeneInterval(contigName: String, start: Int, end: Int, strand: String, geneId: String, geneLength: Int, lowCoverageLength: Int)
