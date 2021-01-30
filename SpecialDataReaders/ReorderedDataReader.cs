using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace SpecialDataReaders
{
	/// <summary>
	/// Used to reorder the columns of a data reader or ignore some
	/// </summary>
	public class ReorderedDataReader : IDataReader
	{
		/// <summary>
		/// Underlying data
		/// </summary>
		protected IDataReader dataReader;
		/// <summary>
		/// Maps going from output to input. (ie: <c>columnMapping[i]</c> is the column number in the underlying data that is being mapped to <c>i</c>).
		/// </summary>
		protected int[] columnMapping;
		/// <summary>
		/// The number of columns
		/// </summary>
		protected int fieldCount;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="dr">The underlying data reader</param>
		/// <param name="columnMap">The mapping from initial index in underlying data reader to the value in the output data reader</param>
		public ReorderedDataReader(IDataReader dr, IList<int?> columnMap)
		{
			dataReader = dr;
			Set(columnMap);
		}

		/// <summary>
		/// Sets the column mapping.
		/// </summary>
		/// <param name="columnMap">The mapping from initial index in underlying data reader to the value in the output data reader (this)</param>
		protected void Set(IList<int?> columnMap)
		{
			columnMapping = new int[dataReader.FieldCount];
			for (int i = 0; i < columnMap.Count; i++)
				if (columnMap[i] is int c)
					columnMapping[c] = i;

			//gap will be counted as the end
			int lastIndex;
			for (lastIndex = 0; lastIndex < columnMapping.Length && columnMapping[lastIndex] >= 0; lastIndex++) { }
			fieldCount = lastIndex;
		}

		///<inheritdoc/>
		public virtual object this[int i] => dataReader[columnMapping[i]];

		///<inheritdoc/>
		public virtual object this[string name] => dataReader[name];

		///<inheritdoc/>
		public virtual int Depth => dataReader.Depth;

		///<inheritdoc/>
		public virtual bool IsClosed => dataReader.IsClosed;

		///<inheritdoc/>
		public virtual int RecordsAffected => dataReader.RecordsAffected;

		///<inheritdoc/>
		public virtual int FieldCount => fieldCount;

		///<inheritdoc/>
		public virtual void Close() => dataReader.Close();
		///<inheritdoc/>
		public virtual void Dispose() => dataReader.Dispose();
		///<inheritdoc/>
		public virtual bool GetBoolean(int i) => dataReader.GetBoolean(columnMapping[i]);
		///<inheritdoc/>
		public virtual byte GetByte(int i) => dataReader.GetByte(columnMapping[i]);
		///<inheritdoc/>
		public virtual long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
		///<inheritdoc/>
		public virtual char GetChar(int i) => dataReader.GetChar(columnMapping[i]);
		///<inheritdoc/>
		public virtual long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
		///<inheritdoc/>
		public virtual IDataReader GetData(int i) => dataReader.GetData(columnMapping[i]);
		///<inheritdoc/>
		public virtual string GetDataTypeName(int i) => dataReader.GetDataTypeName(columnMapping[i]);
		///<inheritdoc/>
		public virtual DateTime GetDateTime(int i) => dataReader.GetDateTime(columnMapping[i]);
		///<inheritdoc/>
		public virtual decimal GetDecimal(int i) => dataReader.GetDecimal(columnMapping[i]);
		///<inheritdoc/>
		public virtual double GetDouble(int i) => dataReader.GetDouble(columnMapping[i]);
		///<inheritdoc/>
		public virtual Type GetFieldType(int i) => dataReader.GetFieldType(columnMapping[i]);
		///<inheritdoc/>
		public virtual float GetFloat(int i) => dataReader.GetFloat(columnMapping[i]);
		///<inheritdoc/>
		public virtual Guid GetGuid(int i) => dataReader.GetGuid(columnMapping[i]);
		///<inheritdoc/>
		public virtual short GetInt16(int i) => dataReader.GetInt16(columnMapping[i]);
		///<inheritdoc/>
		public virtual int GetInt32(int i) => dataReader.GetInt32(columnMapping[i]);
		///<inheritdoc/>
		public virtual long GetInt64(int i) => dataReader.GetInt64(columnMapping[i]);
		///<inheritdoc/>
		public virtual string GetName(int i) => dataReader.GetName(columnMapping[i]);
		///<inheritdoc/>
		public virtual int GetOrdinal(string name) => dataReader.GetOrdinal(name);
		///<inheritdoc/>
		public virtual DataTable GetSchemaTable() => dataReader.GetSchemaTable();
		///<inheritdoc/>
		public virtual string GetString(int i) => dataReader.GetString(columnMapping[i]);
		///<inheritdoc/>
		public virtual object GetValue(int i) => dataReader.GetValue(columnMapping[i]);
		///<inheritdoc/>
		public virtual int GetValues(object[] values)
		{
			int len = Math.Min(values.Length, FieldCount);
			for (int i = 0; i < len; i++)
				values[i] = GetValue(i);
			return len;
		}
		///<inheritdoc/>
		public virtual bool IsDBNull(int i) => dataReader.IsDBNull(columnMapping[i]);
		///<inheritdoc/>
		public virtual bool NextResult() => dataReader.NextResult();
		///<inheritdoc/>
		public virtual bool Read() => dataReader.Read();
	}
}
