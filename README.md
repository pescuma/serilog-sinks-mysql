# serilog-sinks-mysql
A Serilog sink that writes events to MySQL


You need, at least:

```
var logger = new LoggerConfiguration()
                           .WriteTo.MySQL(connectionString)
                           .CreateLogger();
```

If you want the log table to be created automatically:

```
var logger = new LoggerConfiguration()
                           .WriteTo.MySQL(connectionString, autoCreateSqlTable: true)
                           .CreateLogger();
```

If you want to change the log table name:

```
var logger = new LoggerConfiguration()
                           .WriteTo.MySQL(connectionString, tableName: "tab_log")
                           .CreateLogger();
```

If you want to store some properties as columns in the log table:

```
var logger = new LoggerConfiguration()
                           .WriteTo.MySQL(connectionString,
                                          aditionalColumns: cs => cs //
                                                 .AddColumnForProperty("PropertyName", "COLUMN_TYPE", "COLUMN_NAME"))
                           .CreateLogger();
```



