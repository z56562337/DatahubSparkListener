
ThisBuild / version := "1.0.3"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "DatahubSparkListener"
  )

resolvers += ("dsii-public maven" at "http://linxpa-pegit00:8081/repository/maven-public/").withAllowInsecureProtocol(true)
externalResolvers := Resolver.combineDefaultResolvers(resolvers.value.toVector, mavenCentral = false)

libraryDependencies ++= {
  val sparkVer = "2.4.7"
  lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVer % Provided,
    "org.apache.spark" %% "spark-streaming" % sparkVer % Provided,
    "org.apache.spark" %% "spark-hive" % sparkVer % Provided,
    "com.typesafe" % "config" % "1.4.0",
    "mysql" % "mysql-connector-java" % "8.0.27",
    "org.apache.httpcomponents" % "httpclient" % "4.5.14",
    "com.microsoft.azure" % "spark-mssql-connector" % "1.0.2",
    "mysql" % "mysql-connector-java" % "8.0.28",
    "io.acryl" % "datahub-spark-lineage" % "0.10.0-6",
    "org.scalatest" %% "scalatest" % "3.2.2",
    "com.garmin.dsii" % "json-utils_2.11" % "1.3.0"
  )
}


assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.startsWith("META-INF") => MergeStrategy.discard
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
  case PathList("org", "apache", xs@_*) => MergeStrategy.first
  case PathList("org", "jboss", xs@_*) => MergeStrategy.first
  case "about.html" => MergeStrategy.rename
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.codehaus.jackson.**" -> "shadejackson.@1").inAll
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
