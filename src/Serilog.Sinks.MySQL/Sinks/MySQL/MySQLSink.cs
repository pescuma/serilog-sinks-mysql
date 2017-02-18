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
		private readonly List<ColumnConfig> aditionalColumns;

		public MySQLSink(string connectionString, string tableName, int batchSizeLimit, TimeSpan period, IFormatProvider formatProvider,
			bool storeTimestampInUtc, bool autoCreateSqlTable, List<ColumnConfig> aditionalColumns)
			: base(batchSizeLimit, period)
		{
			this.connectionString = connectionString;
			this.tableName = tableName;
			this.formatProvider = formatProvider;
			this.storeTimestampInUtc = storeTimestampInUtc;
			this.aditionalColumns = aditionalColumns;

			if (autoCreateSqlTable)
				CreateTable();
		}

		private void CreateTable()
		{
			using (var connection = new MySqlConnection(connectionString))
			{
				connection.Open();

				string propertiesType;

				if (GetVersion(connection) >= new Version(5, 7, 8))
					propertiesType = "JSON";
				else
					propertiesType = "TEST";

				var sql = new StringBuilder();
				sql.Append("CREATE TABLE IF NOT EXISTS ")
						.Append(tableName)
						.Append(@" (");

				sql.Append("Id INTEGER NOT NULL AUTO_INCREMENT, ");
				sql.Append("Timestamp DATETIME NOT NULL, ");
				sql.Append("Level VARCHAR(20) NOT NULL, ");
				sql.Append("Message TEXT,");
				sql.Append("Exception TEXT, ");

				foreach (ColumnConfig col in aditionalColumns)
					sql.Append(col.Name)
							.Append(" ")
							.Append(col.Type)
							.Append(", ");

				sql.Append("Properties ")
						.Append(propertiesType)
						.Append(", ");

				sql.Append("PRIMARY KEY (Id)");
				sql.Append(");");

				using (var command = new MySqlCommand(sql.ToString(), connection))
				{
					command.ExecuteNonQuery();
				}
			}
		}

		private static Version GetVersion(MySqlConnection connection)
		{
			using (var command = new MySqlCommand("SHOW VARIABLES LIKE \"version\"", connection))
			using (MySqlDataReader reader = command.ExecuteReader())
			{
				if (!reader.Read())
					return new Version();

				string value = (reader[1] ?? "").ToString();

				int pos = value.IndexOf('-');
				if (pos >= 0)
					value = value.Substring(0, pos);

				Version version;
				if (!Version.TryParse(value, out version))
					return new Version();

				return version;
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
					sql.Append("insert into ")
							.Append(tableName)
							.Append("(Timestamp, Level, Exception, Message, Properties");
					foreach (ColumnConfig col in aditionalColumns)
						sql.Append(", ")
								.Append(col.Name);
					sql.Append(")\nvalues ");

					var i = 0;
					foreach (LogEvent logEvent in events)
					{
						if (i > 0)
							sql.Append(",\n");

						// The param names are meaningless to keep the string smaller
						sql.Append($"(@{i}1, @{i}2, @{i}3, @{i}4, @{i}5");
						for (var j = 0; j < aditionalColumns.Count; j++)
							sql.Append($", @{i}{j + 6}");
						sql.Append(")");

						command.Parameters.AddWithValue($"{i}1",
							storeTimestampInUtc ? logEvent.Timestamp.DateTime.ToUniversalTime() : logEvent.Timestamp.DateTime);
						command.Parameters.AddWithValue($"{i}2", logEvent.Level.ToString());
						command.Parameters.AddWithValue($"{i}3", logEvent.Exception != null ? logEvent.Exception.ToString() : "");
						command.Parameters.AddWithValue($"{i}4", logEvent.RenderMessage(formatProvider));
						command.Parameters.AddWithValue($"{i}5", ToJson(logEvent.Properties));

						for (var j = 0; j < aditionalColumns.Count; j++)
							command.Parameters.AddWithValue($"{i}{j + 6}", GetProperty(logEvent.Properties, aditionalColumns[j].Property));

						++i;
					}

					command.Connection = connection;
					command.CommandText = sql.ToString();
					await command.ExecuteNonQueryAsync()
							.ConfigureAwait(false);
				}
			}
		}

		private object GetProperty(IReadOnlyDictionary<string, LogEventPropertyValue> properties, string name)
		{
			LogEventPropertyValue value;
			if (!properties.TryGetValue(name, out value))
				return null;

			var scalar = value as ScalarValue;
			if (scalar != null)
			{
				return scalar.Value;
			}
			else
			{
				using (var result = new StringWriter())
				{
					var formater = new JsonValueFormatter();
					formater.Format(value, result);

					return result.ToString();
				}
			}
		}

		private string ToJson(IReadOnlyDictionary<string, LogEventPropertyValue> properties)
		{
			if (properties.Count < 1)
				return "";

			var formater = new JsonValueFormatter();

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

	public class ColumnConfig
	{
		public string Name;
		public string Type;
		public string Property;
	}
}
