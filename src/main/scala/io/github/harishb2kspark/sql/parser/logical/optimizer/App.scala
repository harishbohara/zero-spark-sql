package io.github.harishb2kspark.sql.parser.logical.optimizer


class NormalClass(var firstName: String, var lastName: String) {

  def describe(): String = {
    "NormalClass - " + firstName + " " + lastName;
  }
}


object App {

  def main(args: Array[String]): Unit = {
    val v: PartialFunction[String, Integer] = {
      case s: String => Integer.parseInt(s)
    }
    if (v.isDefinedAt("101")) {
      val p = v.apply("101")
      println(p)
    }

    var obj: NormalClass = null
    obj = new NormalClass("harish", "bohara")
    obj.firstName = "aaa"


    obj match {
      /*case c@CaseClass(f, l) =>
        println(f + " " + l)
        c.describe()
*/
      case NormalClass(_, _) =>
        println("bad")
    }


    val pf1: PartialFunction[Any, String] = {
      // case s: String if s.nonEmpty => "nonEmpty"
      // case s: String => "empty"
      // case CaseClass(f, l) => f + " " + l
      case n@NormalClass(firstName, lastName) => "Got it " + firstName + " " + lastName + n.describe()
    }

    println(pf1(obj))
  }
}

object NormalClass {
  def unapply(nc: NormalClass): Option[(String, String)] = {
    Some(nc.firstName, nc.lastName)
  }
}

/*

case class CaseClass(override val firstName: String, override val lastName: String) extends NormalClass {
  override def describe(): String = {
    "CaseClass - " + firstName + " " + lastName;
  }
}*/
