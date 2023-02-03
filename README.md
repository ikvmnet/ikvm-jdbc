# IKVM.Jdbc

## IKVM.Jdbc.Data

ADO.NET provider for JDBC through IKVM.

Current IKVM issues require that the `JdbcConnection` object be initialized with a `java.sql.Connection` instance. Other
constructors will likely fail to locate the named JDBC driver. This has to do with the class loader hierarchy not being exactly right for the JDBC SPI.
