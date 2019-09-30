name := "genomeqc"

version := "0.1"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers += Resolver.jcenterRepo
resolvers += "biodatageeks-releases" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/"
resolvers += "biodatageeks-snapshots" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/"


val bigWigLibrary = "org.jetbrains.bio" % "big" % "0.8.4"
val sparkVersion = "2.4.2"

libraryDependencies += "org.biodatageeks" % "bdg-sequila_2.11" % "0.5.5-spark-2.4.2" exclude("org.apache.spark",
  "spark-core_2.11") exclude("org.apache.spark", "spark-sql_2.11")

libraryDependencies += bigWigLibrary
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shaded.com.google.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", _*) => MergeStrategy.first
  case PathList("META-INF", _*) => MergeStrategy.discard
  case referenceOverrides if referenceOverrides.contains("reference-overrides.conf") => MergeStrategy.concat
  case PathList("reference-overrides.conf") => MergeStrategy.concat
  case _ => MergeStrategy.first
}

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated