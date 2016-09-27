name := "SQLpt"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
)

libraryDependencies ++= Seq(
  "com.chuusai" %% "shapeless" % "2.3.2",
  "org.specs2" %% "specs2-core" % "3.0" % "test"
)

scalacOptions in Test ++= Seq("-Yrangepos")