name := "lda-spark"

version := "0.1"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "1.6.1" % "test",
  "org.scalanlp" % "chalk" % "1.3.0",
  "org.apache" % "spark" % "0.9.1"
)
