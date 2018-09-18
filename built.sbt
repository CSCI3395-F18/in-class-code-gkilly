updateOptions := updateOptions.value.withLatestSnapshots(false)

lazy val root = (project in file("."))
  .settings(
    name         := "BigData",
    organization := "edu.trinity",
    scalaVersion := "2.11.12",
    version      := "0.1.0-SNAPSHOT",
		libraryDependencies += "edu.trinity" %% "swiftvis2" % "0.1.0-SNAPSHOT",
		libraryDependencies += "edu.trinity" %% "swiftvis2spark" % "0.1.0-SNAPSHOT",
	//	libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "0.9.1",
	//	libraryDependencies += "org.deeplearning4j" % "deeplearning4j-core" % "0.9.1",
	//	libraryDependencies += "org.deeplearning4j" % "dl4j-spark_2.11" % "0.9.1_spark_2",
	//	libraryDependencies += "org.datavec" % "datavec-api" % "0.9.1",

		libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1",
	//	libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1",
	//	libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.1",
	//	libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.3.1"
//		libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1" % "provided",
//		libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1" % "provided",
//		libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.1" % "provided",
//		libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.3.1" % "provided"
  )
