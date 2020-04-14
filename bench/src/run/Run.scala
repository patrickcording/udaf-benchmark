package run

case class Run(character: Char, length: Long)

case object Run {
  // Todo: Can this be more Scala-ish?
  def getLongestRun(s: String): Option[Run] = {
    var currentChar: Char = 'a'
    var currentLen = 0
    var result: Option[Run] = None

    for (c <- s) {
      if (currentLen == 0) {
        currentChar = c
        currentLen = 1
      } else if (currentChar == c) {
        currentLen += 1
      } else {
        currentChar = c
        currentLen = 1
      }

      result = result match {
        case Some(r) if r.length >= currentLen => Some(r)
        case _ => Some(Run(currentChar, currentLen))
      }
    }

    result
  }

  def getLongestRun(r1: Run, r2: Run): Run = {
    if (r1.length > r2.length) r1
    else r2
  }
}
