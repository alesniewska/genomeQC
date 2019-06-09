package model

case class AnnotatedInterval(contigName: String, start: Int, end: Int, annotation: Float) extends ContigInterval {
}
