package sqlpt.hive.test

import java.util.StringTokenizer

import org.specs2.matcher.{Expectable, MatchResult, Matcher}
import sqlpt.hive.Hql
import hivevalid.HiveValid
import HiveValid.CompileResult._

trait Helpers {
  case class beSameHqlAs(expected: Hql) extends Matcher[Hql] {
    override def apply[S <: Hql](self: Expectable[S]): MatchResult[S] = {
      val expectedCompilation = HiveValid.compile(expected)
      val selfCompilation     = HiveValid.compile(self.value)
      val areTheSame          = tokenized(self.value.trim) == tokenized(expected.trim)

      val results = (expectedCompilation, areTheSame, selfCompilation)

      def failureMessage = results match {
        case (Failure(msg), _, _) =>
          s"The expected query:\n\n'${expected.trim}'\n\ndid not compile: $msg"
        case (Success, false, _) =>
          s"${self.value.trim}\n\nwas not the same HiveQL as:\n\n${expected.trim}"
        case other =>
          sys error s"Unexpected results: $results"
      }

      result(
        results == (Success, true, Success),
        "The queries compiled and matched",
        failureMessage,
        self)
    }

    private def tokenized(hql: Hql): List[String] = {
      val Delimiters = " \n()[]{},;:"

      def toList(t: StringTokenizer): List[String] =
        if(t.hasMoreTokens)
          t.nextToken :: toList(t)
        else
          Nil

      toList(new StringTokenizer(hql, Delimiters, true)).filterNot(_.trim.isEmpty)
    }
  }
}
