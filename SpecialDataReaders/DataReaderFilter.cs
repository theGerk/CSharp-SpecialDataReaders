using System;
using System.Data;

namespace Gerk.SpecialDataReaders
{
	//Data reader to let you filter columns out a data reader. May want to rethink how this is designed to bring it more inline with other implementations in this file.
	/// <summary>
	/// Wraps another datareader filtering out specific rows based on a predicate.
	/// </summary>
	/// <typeparam name="T">The underlying datareader type</typeparam>
	public class DataReaderFilter<T> : IDataReader where T : IDataReader
	{
		readonly T me;
		readonly Predicate<T> include;

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="dataReader">The underlying datareader.</param>
		/// <param name="where">Predicate that filters rows. Only keep the rows where this returns true. The argument will be the datareader once it has progressed to the current point.</param>
		public DataReaderFilter(T dataReader, Predicate<T> where)
		{
			me = dataReader;
			include = where;
		}

		/// <inheritdoc/>
		public object this[int i] => me[i];

		/// <inheritdoc/>
		public object this[string name] => me[name];

		/// <inheritdoc/>
		public int Depth => me.Depth;

		/// <inheritdoc/>
		public bool IsClosed => me.IsClosed;

		/// <inheritdoc/>
		public int RecordsAffected => me.RecordsAffected;

		/// <inheritdoc/>
		public int FieldCount => me.FieldCount;

		/// <inheritdoc/>
		public void Close() => me.Close();

		/// <inheritdoc/>
		public void Dispose() => me.Dispose();

		/// <inheritdoc/>
		public bool GetBoolean(int i) => me.GetBoolean(i);

		/// <inheritdoc/>
		public byte GetByte(int i) => me.GetByte(i);

		/// <inheritdoc/>
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => me.GetBytes(i, fieldOffset, buffer, bufferoffset, length);

		/// <inheritdoc/>
		public char GetChar(int i) => me.GetChar(i);

		/// <inheritdoc/>
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => me.GetChars(i, fieldoffset, buffer, bufferoffset, length);

		/// <inheritdoc/>
		public IDataReader GetData(int i) => me.GetData(i);

		/// <inheritdoc/>
		public string GetDataTypeName(int i) => me.GetDataTypeName(i);

		/// <inheritdoc/>
		public DateTime GetDateTime(int i) => me.GetDateTime(i);

		/// <inheritdoc/>
		public decimal GetDecimal(int i) => me.GetDecimal(i);

		/// <inheritdoc/>
		public double GetDouble(int i) => me.GetDouble(i);

		/// <inheritdoc/>
		public Type GetFieldType(int i) => me.GetFieldType(i);

		/// <inheritdoc/>
		public float GetFloat(int i) => me.GetFloat(i);

		/// <inheritdoc/>
		public Guid GetGuid(int i) => me.GetGuid(i);

		/// <inheritdoc/>
		public short GetInt16(int i) => me.GetInt16(i);

		/// <inheritdoc/>
		public int GetInt32(int i) => me.GetInt32(i);

		/// <inheritdoc/>
		public long GetInt64(int i) => me.GetInt64(i);

		/// <inheritdoc/>
		public string GetName(int i) => me.GetName(i);

		/// <inheritdoc/>
		public int GetOrdinal(string name) => me.GetOrdinal(name);

		/// <inheritdoc/>
		public DataTable GetSchemaTable() => me.GetSchemaTable();

		/// <inheritdoc/>
		public string GetString(int i) => me.GetString(i);

		/// <inheritdoc/>
		public object GetValue(int i) => me.GetValue(i);

		/// <inheritdoc/>
		public int GetValues(object[] values) => me.GetValues(values);

		/// <inheritdoc/>
		public bool IsDBNull(int i) => me.IsDBNull(i);

		/// <inheritdoc/>
		public bool NextResult() => me.NextResult();

		/// <inheritdoc/>
		public bool Read()
		{
			//I know this is hard to read, but I couldn't think of a better way to do this.
			//keep going until either we hit the end (me.Read() is False) or we find a record to include (include(me) is True)
			//then return if we hit the end as normal
			bool output;
			while ((output = me.Read()) && !include(me)) { }
			return output;
		}
	}
}
