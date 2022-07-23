package io.github.harishb2kspark.sql.parser.logical.optimizer

object App {

  def main(args: Array[String]): Unit = {
    val v: PartialFunction[String, Integer] = {
      case s: String => Integer.parseInt(s)
    }
    if (v.isDefinedAt("101")) {
      val p = v.apply("101")
      println(p)
    }
  }
}

