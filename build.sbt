name := "Featurer"

organization := "com.firemindlabs"

version := "1.0-SNAPSHOT"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.3" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

// https://mvnrepository.com/artifact/com.github.scopt/scopt
libraryDependencies += "com.github.scopt" %% "scopt" % "3.6.0"
libraryDependencies += "org.yaml" % "snakeyaml" % "1.24"

parallelExecution in Test := false

publishMavenStyle := true

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }


assemblyMergeStrategy in assembly := {
  case x if x.startsWith("META-INF") => MergeStrategy.discard // Bumf
  case x if x.endsWith(".html") => MergeStrategy.discard // More bumf
  case x if x.contains("slf4j-api") => MergeStrategy.last
  case x if x.contains("slf4j") => MergeStrategy.first
  case x if x.contains("commons") => MergeStrategy.first
  case x if x.contains("web-app") => MergeStrategy.first
  case x if x.contains("datatypes") => MergeStrategy.first
  case x if x.contains("XMLSchema") => MergeStrategy.first
  case x if x.contains("xsd") => MergeStrategy.first
  case x if x.contains("Http") => MergeStrategy.first
  case x if x.contains("org/cyberneko/html") => MergeStrategy.first
  case x if x.contains("parquet") => MergeStrategy.first
  case x if x.contains("class") => MergeStrategy.first
  case x if x.contains("properties") => MergeStrategy.first
  case x if x.contains("xml") => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs@_ *) => MergeStrategy.last // For Log$Logger.class
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

publishArtifact in Test := false


