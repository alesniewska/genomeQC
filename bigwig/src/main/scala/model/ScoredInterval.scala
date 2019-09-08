package model

class ScoredInterval(contigName: String, start: Int, end: Int, val score: Float) extends SimpleInterval(contigName, start, end) {
}
