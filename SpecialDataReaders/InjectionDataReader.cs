using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Gerk.LinqExtensions;

namespace Gerk.SpecialDataReaders
{
	/// <summary>
	/// Allows logic to executed upon reading of each row.
	/// </summary>
	/// <typeparam name="T">Underlying datareader type</typeparam>
	public class InjectionDataReader<T> : IDataReader where T : IDataReader
	{
		/// <summary>
		/// Helper method, makes enumerator that infinitly returns the same value.
		/// </summary>
		/// <typeparam name="U"></typeparam>
		/// <param name="original"></param>
		/// <returns></returns>
		private static IEnumerator<U> InfiniteOf<U>(U original)
		{
			while (true)
				yield return original;
		}

		private readonly T data;
		private readonly IEnumerator<Action<T>> readInjection;

		/// <summary>
		/// Constructs injection datareader
		/// </summary>
		/// <param name="underlyingDataReader">Underlying datareader.</param>
		/// <param name="injection">Function is called on each call of <see cref="Read"/>.</param>
		public InjectionDataReader(T underlyingDataReader, Action<T> injection)
		{
			readInjection = InfiniteOf(injection);
			readInjection.MoveNext();
			data = underlyingDataReader;
		}

		/// <summary>
		/// Constructs injection datareader
		/// </summary>
		/// <param name="underlyingDataReader">Underlying datareader.</param>
		/// <param name="injection">The injected functions that get called on each read. Only one of these is used at a time, and each call to <see cref="InjectionDataReader{T}.NextResult"/> steps to the next function.</param>
		public InjectionDataReader(T underlyingDataReader, params Action<T>[] injection)
		{
			readInjection = injection.AsEnumerable().GetEnumerator();
			readInjection.MoveNext();
			data = underlyingDataReader;
		}

		/// <summary>
		/// Constructs injection datareader
		/// </summary>
		/// <param name="underlyingDataReader">Underlying datareader.</param>
		/// <param name="injection">The injected functions that get called on each read. Only one of these is used at a time, and each call to <see cref="InjectionDataReader{T}.NextResult"/> steps to the next function.</param>
		public InjectionDataReader(T underlyingDataReader, IEnumerator<Action<T>> injection)
		{
			readInjection = injection;
			readInjection.MoveNext();
			data = underlyingDataReader;
		}

		/// <inheritdoc/>
		public object this[int i] => data[i];

		/// <inheritdoc/>
		public object this[string name] => data[name];

		/// <inheritdoc/>
		public int Depth => data.Depth;

		/// <inheritdoc/>
		public bool IsClosed => data.IsClosed;

		/// <inheritdoc/>
		public int RecordsAffected => data.RecordsAffected;

		/// <inheritdoc/>
		public int FieldCount => data.FieldCount;

		/// <inheritdoc/>
		public void Close() => data.Close();

		/// <inheritdoc/>
		public void Dispose() => data.Dispose();

		/// <inheritdoc/>
		public bool GetBoolean(int i) => data.GetBoolean(i);

		/// <inheritdoc/>
		public byte GetByte(int i) => data.GetByte(i);

		/// <inheritdoc/>
		public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => data.GetBytes(i, fieldOffset, buffer, bufferoffset, length);

		/// <inheritdoc/>
		public char GetChar(int i) => data.GetChar(i);

		/// <inheritdoc/>
		public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => data.GetChars(i, fieldoffset, buffer, bufferoffset, length);

		/// <inheritdoc/>
		public IDataReader GetData(int i) => data.GetData(i);

		/// <inheritdoc/>
		public string GetDataTypeName(int i) => data.GetDataTypeName(i);

		/// <inheritdoc/>
		public DateTime GetDateTime(int i) => data.GetDateTime(i);

		/// <inheritdoc/>
		public decimal GetDecimal(int i) => data.GetDecimal(i);

		/// <inheritdoc/>
		public double GetDouble(int i) => data.GetDouble(i);

		/// <inheritdoc/>
		public Type GetFieldType(int i) => data.GetFieldType(i);

		/// <inheritdoc/>
		public float GetFloat(int i) => data.GetFloat(i);

		/// <inheritdoc/>
		public Guid GetGuid(int i) => data.GetGuid(i);

		/// <inheritdoc/>
		public short GetInt16(int i) => data.GetInt16(i);

		/// <inheritdoc/>
		public int GetInt32(int i) => data.GetInt32(i);

		/// <inheritdoc/>
		public long GetInt64(int i) => data.GetInt64(i);

		/// <inheritdoc/>
		public string GetName(int i) => data.GetName(i);

		/// <inheritdoc/>
		public int GetOrdinal(string name) => data.GetOrdinal(name);

		/// <inheritdoc/>
		public DataTable GetSchemaTable() => data.GetSchemaTable();

		/// <inheritdoc/>
		public string GetString(int i) => data.GetString(i);

		/// <inheritdoc/>
		public object GetValue(int i) => data.GetValue(i);

		/// <inheritdoc/>
		public int GetValues(object[] values) => data.GetValues(values);

		/// <inheritdoc/>
		public bool IsDBNull(int i) => data.IsDBNull(i);

		/// <inheritdoc/>
		public bool NextResult()
		{
			readInjection.MoveNext();
			return data.NextResult();
		}


		/// <inheritdoc/>
		public bool Read()
		{
			bool output = data.Read();
			if (output)
				readInjection.Current(data);
			return output;
		}
	}
}
