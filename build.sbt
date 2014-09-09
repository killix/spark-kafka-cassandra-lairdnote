import AssemblyKeys._

assemblySettings

name := "spark-c4hcdn-kafka"

version := "1.0"

scalaVersion := "2.10.4"


jarName in assembly := "spark-c4hcdn-kafka_2.10-1.0.jar"

assemblyOption in assembly ~= { _.copy(includeScala = false) }

libraryDependencies ++= Seq(
  "com.esotericsoftware.kryo" % "kryo" % "2.10" % "provided",
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.0.0-rc5" withSources() withJavadoc(),
  "org.apache.spark" %% "spark-core" % "1.0.2" % "provided",
  "net.debasishg" % "redisclient_2.10" % "2.12",
  "org.apache.spark" %% "spark-streaming" % "1.0.2" % "provided",
  ("org.apache.spark" %% "spark-streaming-kafka" % "1.0.2").
    exclude("commons-beanutils", "commons-beanutils").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")
)


mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case x if x.startsWith("META-INF/ECLIPSEF.RSA") => MergeStrategy.last
    case x if x.startsWith("META-INF/mailcap") => MergeStrategy.last
    case x if x.startsWith("plugin.properties") => MergeStrategy.last
    case x if x.startsWith("org/apache/commons/logging") => MergeStrategy.first
    case x if x.startsWith("akka") => MergeStrategy.first
    case x => old(x)
  }
}
