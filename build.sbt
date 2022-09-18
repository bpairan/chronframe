
lazy val scala212 = "2.12.15"
lazy val scala213 = "2.13.8"
lazy val supportedScalaVersions = List(scala212, scala213)

ThisBuild / scalaVersion := scala213

ThisBuild / organization := "io.github.bpairan"

ThisBuild / organizationName := "bpairan"

resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/releases"

coverageEnabled := true

crossPaths := true
crossScalaVersions := supportedScalaVersions

// logger dependencies
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "org.slf4j" % "slf4j-simple" % "1.7.36"
)

//Chronicle dependency
libraryDependencies += "net.openhft" % "chronicle-queue" % "5.22.18"

libraryDependencies += "org.typelevel" %% "cats-core" % "2.7.0"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.12.0"
libraryDependencies += "com.google.guava" % "guava" % "31.1-jre"

//adds support for 2.13 features to older versions of scala
libraryDependencies += "com.github.bigwheel" %% "util-backports" % "2.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % Test
libraryDependencies += "org.mockito" % "mockito-core" % "4.6.1" % Test

Compile / scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, n)) if n == 12 => List("-Ypartial-unification")
    case _ => Nil
  }
}