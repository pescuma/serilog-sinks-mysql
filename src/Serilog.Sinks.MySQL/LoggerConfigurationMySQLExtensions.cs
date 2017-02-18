using System;
using System.Configuration;
using Serilog.Configuration;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.MySQL;

namespace Serilog
{
	public static class LoggerConfigurationMySQLExtensions
	{
		public static LoggerConfiguration MySQL(this LoggerSinkConfiguration loggerConfiguration,
			string nameOrConnectionString, string tableName = MySQLSink.DefaultTableName,
			LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
			int batchPostingLimit = MySQLSink.DefaultBatchPostingLimit, TimeSpan? period = null,
			IFormatProvider formatProvider = null, bool storeTimestampInUtc = false, bool autoCreateSqlTable = false)
		{
			if (loggerConfiguration == null)
				throw new ArgumentNullException(nameof(loggerConfiguration));
			if (nameOrConnectionString == null)
				throw new ArgumentNullException(nameof(nameOrConnectionString));
			if (tableName == null)
				throw new ArgumentNullException(nameof(tableName));

			string connectionString = GetConnectionString(nameOrConnectionString);
			TimeSpan defaultedPeriod = period ?? MySQLSink.DefaultPeriod;

			return
					loggerConfiguration.Sink(
						new MySQLSink(connectionString, tableName, batchPostingLimit, defaultedPeriod, formatProvider, storeTimestampInUtc,
							autoCreateSqlTable), restrictedToMinimumLevel);
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
}