package model

case class NamedInterval(contigName: String, start: Int, end: Int, name: String) extends ContigInterval {
}
