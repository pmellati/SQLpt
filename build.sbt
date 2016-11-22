lazy val Version = new {
  val scala     = "2.11.8"
  val scalaz    = "7.2.6"
  val shapeless = "2.3.2"
  val specs2    = "3.8.5.1"
}

lazy val commonSettings = Seq(
  version := "0.1.1",
  publishMavenStyle := false,

  scalaVersion := Version.scala,

  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
  ),

  conflictManager := ConflictManager.strict,

  scalacOptions ++= Seq(
    "-Xfatal-warnings",
    "-feature",
    "-language:_"
  ),

  scalacOptions in Test ++= Seq("-Yrangepos"),

  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html"))
)

lazy val root = project
  .in(file("."))
  .aggregate(sqlpt, macros, columns, examples)
  .settings(commonSettings)
  .settings(
    name := "sqlpt-root"
  )

lazy val sqlpt = project
  .dependsOn(macros, columns)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % Version.shapeless,

      "org.scalaz" %% "scalaz-core" % Version.scalaz,

      "org.specs2" %% "specs2-core"          % Version.specs2 % "test",
      "org.specs2" %% "specs2-matcher-extra" % Version.specs2 % "test"
    )
  )

lazy val macros = project
  .dependsOn(columns)
  .settings(commonSettings)
  .settings(
    name := "sqlpt-macros",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % Version.scala)
  )

lazy val columns = project
  .settings(commonSettings)
  .settings(
    name := "sqlpt-columns",
    libraryDependencies ++= Seq(
      "org.scalaz" %% "scalaz-core" % Version.scalaz)
  )

lazy val examples = project
  .dependsOn(sqlpt)
  .settings(commonSettings)
  .settings(
    name := "sqlpt-examples"
  )