package genomeqc.model

abstract class ContigInterval {
  def contigName: String
  def start: Int
  def end: Int
}
