package model

case class Gene(contigName: String, start: Int, end: Int, strand: String, geneId: String) extends ContigInterval {
}
