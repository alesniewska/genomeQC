package model

case class ScoredInterval(contigName: String, start: Int, end: Int, score: Float) extends ContigInterval {
}
