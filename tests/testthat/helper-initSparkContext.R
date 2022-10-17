loadSpark <- function(){
    # Getting current Spark installation version
    installedSpark <- sparklyr::spark_installed_versions()
    sparkId <- which(installedSpark$spark == max(installedSpark$spark))
    sparkVersion <- installedSpark[sparkId,"spark"]
    hadoopVersion <- installedSpark[sparkId, "hadoop"]

    Sys.setenv("SPARK_HOME"=installedSpark[1, "dir"])
    
    # Generating spark-submit config
    config <- sparklyr::spark_config()
    config[["sparklyr.shell.driver-memory"]] <- "1G"
    config[["sparklyr.apply.env.foo"]] <- "env-test"
    config[["spark.sql.warehouse.dir"]] <- tempfile()
    config[["spark.sql.extensions"]] <- "io.delta.sql.DeltaSparkSessionExtension"
    config[["spark.sql.catalog.spark_catalog"]] <- "org.apache.spark.sql.delta.catalog.DeltaCatalog"

    if (identical(.Platform$OS.type, "windows")) {
      config[["spark.sql.session.timeZone"]] <- "UTC"
    }
    config$`sparklyr.sdf_collect.persistence_level` <- "NONE"
    packages <- c("io.delta:delta-core_2.12:2.1.0")

    # Getting Spark Context
    sc <- sparklyr::spark_connect(
      master = "local",
      method = "shell",
      version = sparkVersion,
      config = config,
      packages=packages
    )

    return(sc)
}

populateSpark <- function(sc) {
  tableName <- "test"
  databaseName <- "main"

  # Creating database
  DBI::dbSendQuery(sc, paste("CREATE DATABASE IF NOT EXISTS", databaseName))

  if (!tableName %in% DBI::dbListTables(sc, databaseName)) {
    tableContent <- data.frame(a=c(1:10))

    DBI::dbWriteTable(
      sc,
      name=paste(databaseName, tableName, sep="."),
      value=tableContent
    )
  }
}

loadAndPopulateSpark <- function () {
  sc <- loadSpark()
  populateSpark(sc)

  return(sc)
}