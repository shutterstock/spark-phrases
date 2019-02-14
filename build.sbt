name := "phraser"
organization := "com.shutterstock"
version := "0.1"
scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"

javacOptions ++= Seq("-encoding", "UTF-8")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
   "org.scalatest" %% "scalatest" % "2.1.5" % "test"
)


wartremoverErrors ++= Warts.allBut(Wart.Any, Wart.TraversableOps, Wart.NonUnitStatements, 
                                   Wart.Throw, Wart.FinalVal, Wart.AsInstanceOf, Wart.Nothing, 
                                   Wart.Overloading, Wart.Var, Wart.Equals, Wart.PublicInference, Wart.Null)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyMergeStrategy in assembly := {
  case PathList("org",   "apache", xs @ _*) => MergeStrategy.last
  case PathList("com",   "google", xs @ _*) => MergeStrategy.last
  case PathList("org",    "aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", xs @ _*) => MergeStrategy.last
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
   oldStrategy(x)
}
