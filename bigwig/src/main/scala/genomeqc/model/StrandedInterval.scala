package genomeqc.model

case class StrandedInterval(contigName: String, start: Int, end: Int, strand: String) extends ContigInterval {
}
