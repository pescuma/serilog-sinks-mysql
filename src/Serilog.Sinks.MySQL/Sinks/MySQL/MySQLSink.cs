using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Serilog.Events;
using Serilog.Formatting.Json;
using Serilog.Sinks.PeriodicBatching;

namespace Serilog.Sinks.MySQL
{
	public class MySQLSink : PeriodicBatchingSink
	{
		public const string DefaultTableName = "Logs";
		public const int DefaultBatchPostingLimit = 100;
		public static readonly TimeSpan DefaultPeriod = TimeSpan.FromSeconds(5);

		private readonly string connectionString;
		private readonly string tableName;
		private readonly IFormatProvider formatProvider;
		private readonly bool storeTimestampInUtc;

		public MySQLSink(string connectionString, string tableName, int batchSizeLimit, TimeSpan period, IFormatProvider formatProvider,
			bool storeTimestampInUtc, bool autoCreateSqlTable)
			: base(batchSizeLimit, period)
		{
			this.connectionString = connectionString;
			this.tableName = tableName;
			this.formatProvider = formatProvider;
			this.storeTimestampInUtc = storeTimestampInUtc;

			if (autoCreateSqlTable)
				CreateTable();
		}

		private void CreateTable()
		{
			string sql = $@"CREATE TABLE IF NOT EXISTS {tableName} (
  Id         INTEGER NOT NULL AUTO_INCREMENT,
  Timestamp  DATETIME,
  Level      VARCHAR(20),
  Exception  TEXT,
  Message    TEXT,
  Properties JSON,

  PRIMARY KEY (Id)
);";

			using (var connection = new MySqlConnection(connectionString))
			{
				connection.Open();

				using (var command = new MySqlCommand(sql, connection))
				{
					command.ExecuteNonQuery();
				}
			}
		}

		protected override async Task EmitBatchAsync(IEnumerable<LogEvent> events)
		{
			using (var connection = new MySqlConnection(connectionString))
			{
				await connection.OpenAsync()
						.ConfigureAwait(false);

				using (var command = new MySqlCommand())
				{
					var sql = new StringBuilder();
					sql.Append($"insert into {tableName}(Timestamp, Level, Exception, Message, Properties)\nvalues ");

					var i = 0;
					foreach (LogEvent logEvent in events)
					{
						if (i > 0)
							sql.Append(",\n");

						// The param names are meaningless to keep the string smaller
						sql.Append($"(@a{i}, @b{i}, @c{i}, @d{i}, @e{i})");

						command.Parameters.AddWithValue($"a{i}",
							storeTimestampInUtc ? logEvent.Timestamp.DateTime.ToUniversalTime() : logEvent.Timestamp.DateTime);
						command.Parameters.AddWithValue($"b{i}", logEvent.Level.ToString());
						command.Parameters.AddWithValue($"c{i}", logEvent.Exception != null ? logEvent.Exception.ToString() : "");
						command.Parameters.AddWithValue($"d{i}", logEvent.RenderMessage(formatProvider));
						command.Parameters.AddWithValue($"e{i}", ToJson(logEvent.Properties));

						++i;
					}

					command.Connection = connection;
					command.CommandText = sql.ToString();
					await command.ExecuteNonQueryAsync()
							.ConfigureAwait(false);
				}
			}
		}

		private string ToJson(IReadOnlyDictionary<string, LogEventPropertyValue> properties)
		{
			if (properties.Count < 1)
				return "";

			var formater = new JsonValueFormatter(typeTagName: "$type");

			using (var result = new StringWriter())
			{
				result.Write('{');
				var i = 0;
				foreach (KeyValuePair<string, LogEventPropertyValue> property in properties)
				{
					if (i > 0)
						result.Write(',');

					result.Write('\n');
					JsonValueFormatter.WriteQuotedJsonString(property.Key, result);
					result.Write(": ");
					formater.Format(property.Value, result);

					++i;
				}
				result.Write("\n}");

				return result.ToString();
			}
		}
	}
}
