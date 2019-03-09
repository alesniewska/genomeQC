name := "bigwig-generator"

version := "0.1"

scalaVersion := "2.11.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers += Resolver.jcenterRepo
resolvers += "biodatageeks-releases" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-releases/"
resolvers += "biodatageeks-snapshots" at "https://zsibio.ii.pw.edu.pl/nexus/repository/maven-snapshots/"

val bigWigDependency = "org.jetbrains.bio" % "big" % "0.8.4"

libraryDependencies += "org.biodatageeks" % "bdg-sequila_2.11" % "0.5.2" % "provided"
libraryDependencies += bigWigDependency

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shaded.com.google.@1").inAll
)

run in Compile := Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)).evaluated
runMain in Compile := Defaults.runMainTask(fullClasspath in Compile, runner in(Compile, run)).evaluated