<Query Kind="Program">
  <Connection>
    <ID>72d7a75b-561b-48c0-8e9c-a3668a01e09b</ID>
    <Persist>true</Persist>
    <Server>DESKTOP-N5KIBUG</Server>
    <Database>StackOverflow2013</Database>
  </Connection>
  <NuGetReference>morelinq</NuGetReference>
  <NuGetReference>Newtonsoft.Json</NuGetReference>
  <NuGetReference>Z.ExtensionMethods.WithNamespace</NuGetReference>
  <Namespace>MoreLinq</Namespace>
  <Namespace>Newtonsoft.Json</Namespace>
  <Namespace>Newtonsoft.Json.Bson</Namespace>
  <Namespace>Newtonsoft.Json.Converters</Namespace>
  <Namespace>Newtonsoft.Json.Linq</Namespace>
  <Namespace>Newtonsoft.Json.Schema</Namespace>
  <Namespace>Newtonsoft.Json.Serialization</Namespace>
  <Namespace>System.Threading.Tasks</Namespace>
  <Namespace>Z.Collections.Extensions</Namespace>
  <Namespace>Z.Core.Extensions</Namespace>
  <Namespace>Z.Data.Extensions</Namespace>
  <Namespace>Z.Reflection.Extensions</Namespace>
  <Namespace>System.Runtime.Serialization</Namespace>
</Query>

void Main()
{
	//USAGE:
	
	var connectionString="[enter connString]";//"Data Source=DESKTOP-N5KIBUG;Integrated Security=SSPI;Initial Catalog=StackOverflow2013;app=LINQPad [AlphaDyne]";
	var targetDatabaseTable="[targetDbName]";//"dbo.AccountInfoTableV";
	var createTableStatementWriter=new SqlCreateTableWriter();
	var filePathToJsonFile ="[path to json file]"; //@"c:\temp\csvfileII.json"; 
	
    var csvLoader= new CsvLoader<AccountInfo,AccountInfoSqlFieldMap>(connectionString,targetDatabaseTable,createTableStatementWriter);

	try
	{
		csvLoader.LoadFile(filePathToJsonFile);
	}
	catch (Exception e) {
		Console.WriteLine(e.Message);
	}
}

//Main executing class. Coordinates loading of the text json file; deserialization; insertion into db.
public class CsvLoader<T, U> where U : SqlFieldFileMap, new() where T : new()
{
	public string ConnectionString { get; }
	public Dictionary<string, string> FieldMap { get; }
	public SqlCreateTableWriter SqlCreateTableWriter { get; }
	public string TargetTableName { get; }

	public CsvLoader(string connectionString, string targetTableName, SqlCreateTableWriter sqlCreateTableWriter)
	{

		this.SqlCreateTableWriter = sqlCreateTableWriter;
		this.ConnectionString = connectionString;
		this.TargetTableName = targetTableName;
	}

	public void LoadFile(string filePath)
	{

		ValidateFilePath(filePath);

		var jsonString = File.ReadAllText(filePath);
		var deserializedResult = JsonConvert.DeserializeObject<List<T>>(jsonString);
		deserializedResult.Dump();
		var dataTable = deserializedResult.ToDataTable();
		dataTable.TableName = this.TargetTableName;

		using (var sqlConnection = new SqlConnection(this.ConnectionString))
		{
			try
			{
				sqlConnection.Open();
				if (DoesTableExist(sqlConnection, dataTable.TableName))
				{
					string sqlTrunc = "TRUNCATE TABLE " + this.TargetTableName;

					using (var sqlCommand = sqlConnection.CreateCommand())
					{

						sqlCommand.CommandText = sqlTrunc;
						sqlCommand.ExecuteNonQuery();

					}
				}
				else
				{
					string sqlTableCreate = this.SqlCreateTableWriter.WriteSqlCreateTableStatement<T, U>(this.TargetTableName);

					using (var sqlCommand = sqlConnection.CreateCommand())
					{
						if (sqlConnection.State == ConnectionState.Closed) { sqlConnection.Open(); }
						sqlCommand.CommandText = sqlTableCreate;
						sqlCommand.ExecuteNonQuery();
					}
				}
				using (SqlTransaction tr = sqlConnection.BeginTransaction())
				{
					try
					{
						using (var bulkInsert = new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, tr))
						{
							for (var c = 0; c < dataTable.Columns.Count; c++)
							{
								bulkInsert.ColumnMappings.Add(dataTable.Columns[c].ColumnName, dataTable.Columns[c].ColumnName);
							}

							bulkInsert.DestinationTableName = this.TargetTableName;
							bulkInsert.WriteToServer(dataTable);
							tr.Commit();
							Console.WriteLine($"Total {dataTable.Rows.Count} records were committed to {dataTable.TableName}");
						}

					}
					catch (Exception e)
					{
						tr.Rollback();
						var hasInnerException = e.InnerException != null;
						var innerExcMessage = hasInnerException ? $"{e.InnerException.Message}" : string.Empty;
						Console.WriteLine($"Bulk Insert Failed. Transaction has been rolled back. Error: {e?.Message}.{innerExcMessage}");
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine($"{e.Message}");
			}
			finally
			{
				if (sqlConnection != null)
				{
					sqlConnection.Close();
				}
			}
		}
	}

	private static void ValidateFilePath(string filePath)
	{
		if (string.IsNullOrEmpty(filePath)) { throw new ArgumentNullException("Provided filePath argument cannot be null."); }

		var providedPathContainsInvalidPathCharacters = Path.GetInvalidPathChars().Intersect(filePath).Count() > 0;

		if (providedPathContainsInvalidPathCharacters) { throw new ArgumentException($"Provided filePath contains invalid path characters: {string.Join(",", Path.GetInvalidPathChars().Intersect(filePath).ToArray())}"); }

		var filePathExists = File.Exists(filePath);

		if (!filePathExists) { throw new FileNotFoundException($"File Path:{filePath} cannot be found"); }

	}

	private static bool DoesTableExist(SqlConnection sqlConnection, string tableName)
	{
		DataTable dataTable = new DataTable();
		if (sqlConnection != null)
		{

			using (var dtAdapter = new SqlDataAdapter($"select * from {tableName}", sqlConnection))
			{
				try
				{


					dtAdapter.FillSchema(dataTable, SchemaType.Source);

					return true;
				}
				catch (Exception e)
				{

					return false;
				}
			}
			return false;
		}
		else
		{
			return false;
		}
	}
}

public abstract class SqlFieldFileMap
{
	public abstract string GetSqlType(string fieldName, Type sourceType);

	public string ToFieldTypeName(string fieldName)
	{
		return $"[{fieldName}]";
	}

}

//Specific mapping for AccountInfo. Swappable. Just redefine GetSqlType output.
public class AccountInfoSqlFieldMap:SqlFieldFileMap {

	public override string GetSqlType(string fieldName, Type sourceType) {

		if (string.IsNullOrEmpty(fieldName) || sourceType == null) { throw new ArgumentNullException($"fieldName is null:{fieldName == null} and sourceType is null: {sourceType==null} must be non-null and non-empty values");}

		if (sourceType.IsNumeric()) return $"{this.ToFieldTypeName(fieldName)} DECIMAL(30,10) NULL";
		
		if(sourceType==typeof(string))return $"{this.ToFieldTypeName(fieldName)} NVARCHAR(1000) NULL";
		
		if(fieldName=="RunDate")return $"{this.ToFieldTypeName(fieldName)} DATE NULL";
		
		if(sourceType==typeof(DateTime))return $"{this.ToFieldTypeName(fieldName)} DATETIME NULL";
		
		return null;
	}

}

//This class writes out list of objects to a Create Table string statement.
public class SqlCreateTableWriter
{

	public string WriteSqlCreateTableStatement<T, UMapper>(string tableName) where UMapper : SqlFieldFileMap, new()
	  																		where T : new()
	{
		var sqlTableFieldStatements = (new T()).GetProperties()
		   .Select(p => new { FieldName = p.Name, FieldType = p.PropertyType.Name, PropType = p.PropertyType })
		   .Select(p => new UMapper().GetSqlType(p.FieldName, p.PropType))
		   .ToArray();

		var sqlTable = string.Join(",", sqlTableFieldStatements).WrapInCreateTableStatement(tableName);

		return sqlTable;
	}
}

//This class is a map to actual dataType in the csv file.
public class AccountInfo
{
	public string AccountNumber { get; set; }
	public string AccountName { get; set; }
	public decimal Balance { get; set; }
	public DateTime RunDate { get; set; }
	public DateTime StartTime { get; set; }

	[OnError]
	internal void OnError(StreamingContext context, ErrorContext errorContext)
	{
		errorContext.Handled = true;

		Console.WriteLine($"Deserialization failed {errorContext.Error.Message}");
	}
}

public static class AppHelpers
{
	public static bool IsNumeric(this object @object)
	{
		if (@object == null) return false;
		if (@object == typeof(string)) return false;
		if (@object == typeof(DateTime)) return false;

		if (@object == typeof(sbyte)) return true;
		if (@object == typeof(byte)) return true;
		if(@object==typeof(short))return true;
		if(@object==typeof(ushort))return true;
		if(@object==typeof(int))return true;
		if(@object==typeof(uint))return true;
		if(@object==typeof(long))return true;
		if(@object==typeof(ulong))return true;
		if(@object==typeof(float))return true;
		if(@object==typeof(double))return true;
		if(@object==typeof(decimal))return true;

        return false;		
	}

	public static string WrapInCreateTableStatement(this  string tableDetails, string tableName)
	{
		var buildTable = $@"CREATE TABLE {tableName}
(
	{tableDetails}
)";
return buildTable;
	}

	public static bool IsTypeString(object type)
	{
		if (type == null) return false;

		if (type.GetType() == typeof(string)) return true;

		return false;

	}
	
	

}