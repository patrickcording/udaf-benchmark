package buffer

import run.Run


class LongestRunBuffer extends Serializable {
  private var longestRun: Option[Run] = None

  def put(s: String): LongestRunBuffer = {
    val thisLongestRun = Run.getLongestRun(s)
    longestRun = (longestRun, thisLongestRun) match {
      case (Some(lr), Some(olr)) => Some(Run.getLongestRun(lr, olr))
      case (lr @ Some(_), None) => lr
      case (None, olr @ Some(_)) => olr
      case (None, None) => None
    }
    this
  }

  def merge(other: LongestRunBuffer): LongestRunBuffer = {
    longestRun = (longestRun, other.longestRun) match {
      case (Some(lr), Some(olr)) => Some(Run.getLongestRun(lr, olr))
      case (lr @ Some(_), None) => lr
      case (None, olr @ Some(_)) => olr
      case (None, None) => None
    }
    this
  }

  def eval: Option[Run] = longestRun
}
