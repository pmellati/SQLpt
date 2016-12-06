package sqlpt.hive.test

import java.util.StringTokenizer

import org.specs2.matcher.{Matcher, MatchersCreation}
import sqlpt.hive.Hql
import hivevalid.HiveValid, HiveValid.CompileResult._

trait Helpers extends MatchersCreation {
  def beSameHqlAs(expected: Hql): Matcher[Hql] = {self: Hql =>
    def tokenized(hql: Hql): List[String] = {
      val Delimiters = " \n()[]{},;:"

      def toList(t: StringTokenizer): List[String] =
        if(t.hasMoreTokens)
          t.nextToken :: toList(t)
        else
          Nil

      toList(new StringTokenizer(hql, Delimiters, true)).filterNot(_.trim.isEmpty)
    }

    val selfCompilation     = HiveValid.compile(self)
    val expectedCompilation = HiveValid.compile(expected)
    val areTheSame          = tokenized(self.trim) == tokenized(expected.trim)

    val results = (expectedCompilation, areTheSame, selfCompilation)

    (
      results match {
        case (Success, true, Success) => true
        case _ => false
      },
      results match {
        case (Failure(msg), _, _) =>
          s"The expected query:\n\n'${expected.trim}'\n\ndid not compile: $msg"
        case (Success, false, _) =>
          s"${self.trim}\n\nwas not the same HiveQL as:\n\n${expected.trim}"
        case other =>
          sys error s"Unexpected results: $results"
      }
    )
  }
}
