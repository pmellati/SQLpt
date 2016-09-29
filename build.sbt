name := "SQLpt"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
)

val Version = new {
  val shapeless = "2.3.2"
}

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % Version.shapeless,
  "org.scalaz" %% "scalaz-core" % "7.1.1",
  "org.specs2" %% "specs2-core" % "3.0" % "test"
)

dependencyOverrides ++= Set(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "com.chuusai" %% "shapeless" % Version.shapeless  // Specs2 depends on a lower version.
)

conflictManager := ConflictManager.strict

scalacOptions ++= Seq(
  "-Xfatal-warnings",
  "-feature",
  "-language:implicitConversions",
  "-language:higherKinds"
)

scalacOptions in Test ++= Seq("-Yrangepos")