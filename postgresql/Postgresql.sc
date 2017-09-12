// Loads the PostgreSQL JDBC driver and creates a `connProperties` value configured with its driver
// name.
//
// Load with the `import $exec` magic import or the `interp.load.module` method.

interp.load.ivy("org.postgresql" % "postgresql" % "42.1.4")

val connProperties = new java.util.Properties
connProperties.put("driver", "org.postgresql.Driver")

println("""|Augment the connection properties in `connProperties`. Then use them to connect to a
           |PostgreSQL database. A Spark SQL example:
           |
           |  connProperties.put("user", "me")
           |  connProperties.put("password", "mypassword")
           |  val connUrl = "jdbc:postgresql://pghost/mydb"
           |  val mytable = spark.read.jdbc(connUrl, "mytable", connProperties)""".stripMargin)
