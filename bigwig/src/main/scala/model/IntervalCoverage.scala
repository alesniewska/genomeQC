package model

case class IntervalCoverage(contigName: String, start: Int, end: Int, coverage: Short) extends ContigInterval {
}
