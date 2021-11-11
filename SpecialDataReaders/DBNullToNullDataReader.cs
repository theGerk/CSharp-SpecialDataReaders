using System;
using System.Data;

namespace Gerk.SpecialDataReaders
{
	internal class DBNullToNullDataReader : IDataReader
	{
		private readonly IDataReader dr;

		public DBNullToNullDataReader(IDataReader dr)
		{
			this.dr = dr;
		}

		public object this[int i] => dr[i].DBNullToNull();

		public object this[string name] => dr[name].DBNullToNull();

		public int Depth => dr.Depth;

		public bool IsClosed => dr.IsClosed;

		public int RecordsAffected => dr.RecordsAffected;

		public int FieldCount => dr.FieldCount;

		public void Close() => dr.Close();
		public void Dispose() => dr.Dispose();
		public bool GetBoolean(int i) => dr.GetBoolean(i);
		public byte GetByte(int i) => dr.GetByte(i);
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => dr.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		public char GetChar(int i) => dr.GetChar(i);
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => dr.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		public IDataReader GetData(int i) => dr.GetData(i);
		public string GetDataTypeName(int i) => dr.GetDataTypeName(i);
		public DateTime GetDateTime(int i) => dr.GetDateTime(i);
		public decimal GetDecimal(int i) => dr.GetDecimal(i);
		public double GetDouble(int i) => dr.GetDouble(i);
		public Type GetFieldType(int i) => dr.GetFieldType(i);
		public float GetFloat(int i) => dr.GetFloat(i);
		public Guid GetGuid(int i) => dr.GetGuid(i);
		public short GetInt16(int i) => dr.GetInt16(i);
		public int GetInt32(int i) => dr.GetInt32(i);
		public long GetInt64(int i) => dr.GetInt64(i);
		public string GetName(int i) => dr.GetName(i);
		public int GetOrdinal(string name) => dr.GetOrdinal(name);
		public DataTable GetSchemaTable() => dr.GetSchemaTable();
		public string GetString(int i) => dr.GetString(i);
		public object GetValue(int i) => dr.GetValue(i).DBNullToNull();
		public int GetValues(object[] values)
		{
			var output = dr.GetValues(values);
			for (int i = 0; i < values.Length; i++)
				values[i] = values.DBNullToNull();
			return output;
		}
		public bool IsDBNull(int i) => dr.IsDBNull(i);
		public bool NextResult() => dr.NextResult();
		public bool Read() => dr.Read();
	}
}
