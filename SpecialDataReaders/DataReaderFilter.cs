using System;
using System.Data;

namespace Gerk.SpecialDataReaders
{
	//Data reader to let you filter columns out a data reader. May want to rethink how this is designed to bring it more inline with other implementations in this file.
	public class DataReaderFilter<T> : IDataReader where T : IDataReader
	{
		T me;
		Predicate<T> include;

		public DataReaderFilter(T dataReader, Predicate<T> where)
		{
			me = dataReader;
			include = where;
		}

		public object this[int i] => me[i];

		public object this[string name] => me[name];

		public int Depth => me.Depth;

		public bool IsClosed => me.IsClosed;

		public int RecordsAffected => me.RecordsAffected;

		public int FieldCount => me.FieldCount;

		public void Close()
		{
			me.Close();
		}

		public void Dispose()
		{
			me.Dispose();
		}

		public bool GetBoolean(int i)
		{
			return me.GetBoolean(i);
		}

		public byte GetByte(int i)
		{
			return me.GetByte(i);
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			return me.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		}

		public char GetChar(int i)
		{
			return me.GetChar(i);
		}

		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			return me.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		}

		public IDataReader GetData(int i)
		{
			return me.GetData(i);
		}

		public string GetDataTypeName(int i)
		{
			return me.GetDataTypeName(i);
		}

		public DateTime GetDateTime(int i)
		{
			return me.GetDateTime(i);
		}

		public decimal GetDecimal(int i)
		{
			return me.GetDecimal(i);
		}

		public double GetDouble(int i)
		{
			return me.GetDouble(i);
		}

		public Type GetFieldType(int i)
		{
			return me.GetFieldType(i);
		}

		public float GetFloat(int i)
		{
			return me.GetFloat(i);
		}

		public Guid GetGuid(int i)
		{
			return me.GetGuid(i);
		}

		public short GetInt16(int i)
		{
			return me.GetInt16(i);
		}

		public int GetInt32(int i)
		{
			return me.GetInt32(i);
		}

		public long GetInt64(int i)
		{
			return me.GetInt64(i);
		}

		public string GetName(int i)
		{
			return me.GetName(i);
		}

		public int GetOrdinal(string name)
		{
			return me.GetOrdinal(name);
		}

		public DataTable GetSchemaTable()
		{
			return me.GetSchemaTable();
		}

		public string GetString(int i)
		{
			return me.GetString(i);
		}

		public object GetValue(int i)
		{
			return me.GetValue(i);
		}

		public int GetValues(object[] values)
		{
			return me.GetValues(values);
		}

		public bool IsDBNull(int i)
		{
			return me.IsDBNull(i);
		}

		public bool NextResult()
		{
			return me.NextResult();
		}

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
