using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace SpecialDataReaders
{
	// Currently exists to be able to add columns to an IDataReader by using a function that takes in the dataReaader at one state and generating computed columns based off that. Will maybe want to rework this to be part of teh functionality of the DataReaderExtender.
	public class InsertionDataReader<T> : IDataReader where T : IDataReader
	{
		private T data;

		public InsertionDataReader(T underlyingDataReader, params Action<T>[] injection)
		{
			readInjection = injection.AsEnumerable().GetEnumerator();
			readInjection.MoveNext();
			data = underlyingDataReader;
		}

		public InsertionDataReader(T underlyingDataReader, IEnumerator<Action<T>> injection)
		{
			readInjection = injection;
			readInjection.MoveNext();
			data = underlyingDataReader;
		}

		public object this[int i] => data[i];

		public object this[string name] => data[name];

		public int Depth => data.Depth;

		public bool IsClosed => data.IsClosed;

		public int RecordsAffected => data.RecordsAffected;

		public int FieldCount => data.FieldCount;

		public void Close()
		{
			data.Close();
		}

		public void Dispose()
		{
			data.Dispose();
		}

		public bool GetBoolean(int i)
		{
			return data.GetBoolean(i);
		}

		public byte GetByte(int i)
		{
			return data.GetByte(i);
		}

		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
		{
			return data.GetBytes(i, fieldOffset, buffer, bufferoffset, length);
		}

		public char GetChar(int i)
		{
			return data.GetChar(i);
		}

		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
		{
			return data.GetChars(i, fieldoffset, buffer, bufferoffset, length);
		}

		public IDataReader GetData(int i)
		{
			return data.GetData(i);
		}

		public string GetDataTypeName(int i)
		{
			return data.GetDataTypeName(i);
		}

		public DateTime GetDateTime(int i)
		{
			return data.GetDateTime(i);
		}

		public decimal GetDecimal(int i)
		{
			return data.GetDecimal(i);
		}

		public double GetDouble(int i)
		{
			return data.GetDouble(i);
		}

		public Type GetFieldType(int i)
		{
			return data.GetFieldType(i);
		}

		public float GetFloat(int i)
		{
			return data.GetFloat(i);
		}

		public Guid GetGuid(int i)
		{
			return data.GetGuid(i);
		}

		public short GetInt16(int i)
		{
			return data.GetInt16(i);
		}

		public int GetInt32(int i)
		{
			return data.GetInt32(i);
		}

		public long GetInt64(int i)
		{
			return data.GetInt64(i);
		}

		public string GetName(int i)
		{
			return data.GetName(i);
		}

		public int GetOrdinal(string name)
		{
			return data.GetOrdinal(name);
		}

		public DataTable GetSchemaTable()
		{
			return data.GetSchemaTable();
		}

		public string GetString(int i)
		{
			return data.GetString(i);
		}

		public object GetValue(int i)
		{
			return data.GetValue(i);
		}

		public int GetValues(object[] values)
		{
			return data.GetValues(values);
		}

		public bool IsDBNull(int i)
		{
			return data.IsDBNull(i);
		}

		public bool NextResult()
		{
			readInjection.MoveNext();
			return data.NextResult();
		}

		private IEnumerator<Action<T>> readInjection;

		public bool Read()
		{
			if (data.Read())
			{
				readInjection.Current(data);
				return true;
			}
			else
			{
				return false;
			}
		}
	}
}
