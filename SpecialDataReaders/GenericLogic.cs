using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Gerk.SpecialDataReaders
{

	#region GeneralCode
	/// <summary>
	/// Generic SQL logic
	/// </summary>
	public static class GenericLogic
	{
		//not sure if this stuff should be public, but its useful in a bunch of places.
		#region Handling DB Nulls
		/// <summary>
		/// Takes an object that may be a <see cref="DBNull.Value"/> and turns it into regular C# <see langword="null"/> if it is.
		/// <seealso cref="NullToDBNull(object)"/>
		/// </summary>
		/// <param name="input">Input object that may be <see cref="DBNull.Value"/>.</param>
		/// <returns>The input object if it is not <see cref="DBNull.Value"/>, and <see langword="null"/> otherwise.</returns>
		public static object DBNullToNull(this object input)
		{
			if (DBNull.Value.Equals(input))
				return null;
			else
				return input;
		}
		/// <summary>
		/// Takes an object that may be a <see langword="null"/> and turns it into <see cref="DBNull.Value"/> for use with a database.
		/// <seealso cref="NullToDBNull(object)"/>
		/// </summary>
		/// <param name="input">Input object that may be <see langword="null"/>.</param>
		/// <returns>The input object if it is not <see langword="null"/>, and <see cref="DBNull.Value"/> otherwise.</returns>
		public static object NullToDBNull(this object input) => input ?? DBNull.Value;
		#endregion

		//List made using https://docs.microsoft.com/en-us/dotnet/framework/data/adonet/sql-server-data-type-mappings
		private readonly static Dictionary<Type, string> sqlServerType = new Dictionary<Type, string>()
		{
			[typeof(long)] = "bigint",
			[typeof(byte[])] = "varbinary",
			[typeof(bool)] = "bit",
			[typeof(string)] = "nvarchar",
			[typeof(char[])] = "nvarchar",
			[typeof(DateTime)] = "date",
			[typeof(DateTimeOffset)] = "datetimeoffset",
			[typeof(decimal)] = "decimal",
			[typeof(double)] = "float",
			[typeof(int)] = "int",
			[typeof(float)] = "real",
			[typeof(short)] = "smallint",
			[typeof(TimeSpan)] = "time",
			[typeof(Guid)] = "uniqueidentifier",
		};
	}

	#endregion
}
