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
    "-deprecation",
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
      "org.specs2" %% "specs2-matcher-extra" % Version.specs2 % "test",

      noHadoop("org.apache.hadoop" % "hadoop-client" % "2.5.0-mr1-cdh5.3.8"),
      noHadoop("org.apache.hive"   % "hive-exec"     % "0.13.1-cdh5.3.8"),
      noHadoop("org.apache.hive"   % "hive-service"  % "0.13.1-cdh5.3.8")   // TODO: can we remove this?
    ) ++ hadoopClasspath
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

def noHadoop(module: ModuleID) = module.copy(
  exclusions = module.exclusions ++ hadoopCP.exclusions
)

def hadoopClasspath = hadoopCP.modules.map(m => m % "provided" intransitive)

lazy val hadoopCP = new {
  // These versions should track the current cluster environment (but do not fear; experience
  // suggests that everything will still work if they are a few point versions behind).
  //
  // Updating them is a manual process. For example, inspect the names of the files on the
  // hadoop classpath by running this command on the integration server:
  //     hadoop classpath | xargs -d ':' -L 1 -i bash -c "echo {}" | tr ' ' '\n'
  // If the filename does not contain version, check inside the jar:
  //     unzip -p /usr/lib/hadoop//parquet-avro.jar \*/MANIFEST.MF \*/pom.properties
  val modules = List[ModuleID](
    "org.apache.hadoop"            % "hadoop-core"               % "2.5.0-mr1-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-tools"              % "2.5.0-mr1-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-annotations"        % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-auth"               % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-common"             % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-hdfs"               % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-hdfs-nfs"           % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-nfs"                % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-yarn-api"           % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-yarn-client"        % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-yarn-common"        % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-yarn-server-common" % "2.5.0-cdh5.3.8",
    "com.twitter"                  % "parquet-avro"              % "1.5.0-cdh5.3.8",
    "com.twitter"                  % "parquet-column"            % "1.5.0-cdh5.3.8",
    "com.twitter"                  % "parquet-common"            % "1.5.0-cdh5.3.8",
    "com.twitter"                  % "parquet-encoding"          % "1.5.0-cdh5.3.8",
    "com.twitter"                  % "parquet-generator"         % "1.5.0-cdh5.3.8",
    "com.twitter"                  % "parquet-hadoop"            % "1.5.0-cdh5.3.8",
    "com.twitter"                  % "parquet-jackson"           % "1.5.0-cdh5.3.8",
    "com.twitter"                  % "parquet-format"            % "2.1.0-cdh5.3.8",
    "org.slf4j"                    % "slf4j-api"                 % "1.7.5",
    "org.slf4j"                    % "slf4j-log4j12"             % "1.7.5",
    "log4j"                        % "log4j"                     % "1.2.17",
    "commons-beanutils"            % "commons-beanutils"         % "1.7.0",
    "commons-beanutils"            % "commons-beanutils-core"    % "1.8.0",
    "commons-cli"                  % "commons-cli"               % "1.2",
    "commons-codec"                % "commons-codec"             % "1.4",
    "commons-collections"          % "commons-collections"       % "3.2.1",
    "org.apache.commons"           % "commons-compress"          % "1.4.1",
    "commons-configuration"        % "commons-configuration"     % "1.6",
    "commons-daemon"               % "commons-daemon"            % "1.0.13",
    "commons-digester"             % "commons-digester"          % "1.8",
    "commons-el"                   % "commons-el"                % "1.0",
    "commons-httpclient"           % "commons-httpclient"        % "3.1",
    "commons-io"                   % "commons-io"                % "2.4",
    "commons-lang"                 % "commons-lang"              % "2.6",
    "commons-logging"              % "commons-logging"           % "1.1.3",
    "commons-net"                  % "commons-net"               % "3.1",
    "org.apache.commons"           % "commons-math3"             % "3.1.1",
    "org.apache.httpcomponents"    % "httpclient"                % "4.2.5",
    "org.apache.httpcomponents"    % "httpcore"                  % "4.2.5",
    "org.apache.avro"              % "avro"                      % "1.7.6-cdh5.3.8",
    "org.apache.zookeeper"         % "zookeeper"                 % "3.4.5-cdh5.3.8",
    "com.google.code.findbugs"     % "jsr305"                    % "1.3.9",
    "com.google.guava"             % "guava"                     % "11.0.2",
    "com.google.protobuf"          % "protobuf-java"             % "2.5.0",
    "com.google.inject"            % "guice"                     % "3.0",
    "com.google.inject.extensions" % "guice-servlet"             % "3.0",
    "org.codehaus.jackson"         % "jackson-mapper-asl"        % "1.8.8",
    "org.codehaus.jackson"         % "jackson-core-asl"          % "1.8.8",
    "org.codehaus.jackson"         % "jackson-jaxrs"             % "1.8.8",
    "org.codehaus.jackson"         % "jackson-xc"                % "1.8.8",
    "org.codehaus.jettison"        % "jettison"                  % "1.1",
    "org.xerial.snappy"            % "snappy-java"               % "1.0.4.1",
    "junit"                        % "junit"                     % "4.11",
    "jline"                        % "jline"                     % "0.9.94",
    "org.mortbay.jetty"            % "jetty"                     % "6.1.26.cloudera.4",
    "org.mortbay.jetty"            % "jetty-util"                % "6.1.26.cloudera.4",
    "hsqldb"                       % "hsqldb"                    % "1.8.0.10",
    "ant-contrib"                  % "ant-contrib"               % "1.0b3",
    "aopalliance"                  % "aopalliance"               % "1.0",
    "javax.inject"                 % "javax.inject"              % "1",
    "javax.xml.bind"               % "jaxb-api"                  % "2.2.2",
    "com.sun.xml.bind"             % "jaxb-impl"                 % "2.2.3-1",
    "javax.servlet"                % "servlet-api"               % "2.5",
    "javax.xml.stream"             % "stax-api"                  % "1.0-2",
    "javax.activation"             % "activation"                % "1.1",
    "com.sun.jersey"               % "jersey-client"             % "1.9",
    "com.sun.jersey"               % "jersey-core"               % "1.9",
    "com.sun.jersey"               % "jersey-server"             % "1.9",
    "com.sun.jersey"               % "jersey-json"               % "1.9",
    "com.sun.jersey.contribs"      % "jersey-guice"              % "1.9",
    "org.fusesource.leveldbjni"    % "leveldbjni-all"            % "1.8",
    "asm"                          % "asm"                       % "3.2",
    "io.netty"                     % "netty"                     % "3.6.2.Final"
  )

  // Different versions of these jars have different organizations. Could do
  // something complicated to change old version to new, but for now just
  // keep a list of alternate versions so can exclude both versions
  val alternateVersions = List[ModuleID](
    "org.ow2.asm"                  % "asm"                       % "4.1",
    "org.jboss.netty"              % "netty"                     % "3.2.2.Final",
    "stax"                         % "stax-api"                  % "1.0.1"
  )

  // These jars have classes which interfere with the classes provided by hadoop
  // so we exclude these as well
  val interferingModules = List[ModuleID](
    "org.apache.hadoop"            % "hadoop-mapreduce-client-core"   % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-mapreduce-client-common" % "2.5.0-cdh5.3.8",
    "org.apache.hadoop"            % "hadoop-client"                  % "2.5.0-cdh5.3.8"
  )

  val exclusions =
    (modules ++ alternateVersions ++ interferingModules)
      .map(m => ExclusionRule(m.organization, m.name))
}