using System;
using System.Collections.Generic;
using System.Configuration;
using Serilog.Configuration;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.MySQL;

namespace Serilog
{
	public static class LoggerConfigurationMySQLExtensions
	{
		public static LoggerConfiguration MySQL(this LoggerSinkConfiguration loggerConfiguration, string nameOrConnectionString,
			string tableName = MySQLSink.DefaultTableName, LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
			int batchPostingLimit = MySQLSink.DefaultBatchPostingLimit, TimeSpan? period = null, IFormatProvider formatProvider = null,
			bool storeTimestampInUtc = false, bool autoCreateSqlTable = false, Action<MySQLColumnsConfig> aditionalColumns = null)
		{
			if (loggerConfiguration == null)
				throw new ArgumentNullException(nameof(loggerConfiguration));
			if (nameOrConnectionString == null)
				throw new ArgumentNullException(nameof(nameOrConnectionString));
			if (tableName == null)
				throw new ArgumentNullException(nameof(tableName));

			string connectionString = GetConnectionString(nameOrConnectionString);
			TimeSpan defaultedPeriod = period ?? MySQLSink.DefaultPeriod;

			var cols = new MySQLColumnsConfig();
			if (aditionalColumns != null)
				aditionalColumns(cols);

			return
					loggerConfiguration.Sink(
						new MySQLSink(connectionString, tableName, batchPostingLimit, defaultedPeriod, formatProvider, storeTimestampInUtc, autoCreateSqlTable,
							cols.CreateColumns()), restrictedToMinimumLevel);
		}

		private static string GetConnectionString(string nameOrConnectionString)
		{
			// If there is an `=`, we assume this is a raw connection string not a named value
			// If there are no `=`, attempt to pull the named value from config
			if (nameOrConnectionString.IndexOf('=') < 0)
			{
				ConnectionStringSettings cs = ConfigurationManager.ConnectionStrings[nameOrConnectionString];
				if (cs != null)
					return cs.ConnectionString;

				SelfLog.WriteLine(
					"MySQL sink configured value {0} is not found in ConnectionStrings settings and does not appear to be a raw connection string.",
					nameOrConnectionString);
			}

			return nameOrConnectionString;
		}
	}

	public class MySQLColumnsConfig
	{
		private readonly List<ColumnConfig> cols = new List<ColumnConfig>();

		public MySQLColumnsConfig AddColumnForProperty(string property, string type = null, string columnName = null)
		{
			if (string.IsNullOrWhiteSpace(property))
				throw new ArgumentNullException(nameof(property));

			if (string.IsNullOrWhiteSpace(type))
				type = "TEXT";
			if (string.IsNullOrWhiteSpace(columnName))
				columnName = property;

			cols.Add(new ColumnConfig
			{
				Name = columnName,
				Type = type,
				Property = property
			});

			return this;
		}

		internal List<ColumnConfig> CreateColumns()
		{
			return cols;
		}
	}
}
