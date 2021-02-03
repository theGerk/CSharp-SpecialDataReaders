using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace SpecialDataReaders
{
	/// <summary>
	/// This implementation of an IDataReader is completely backed by IEnumerator(s). It may be populated with IEnumerables or IEnumerators and will keep reading data until any of the enumerators end. This is particularly useful for Bulk Data inserts from C# data.
	/// </summary>
	public class EnumeratorDataReader : IDataReader
	{
		/// <summary>
		/// Maps from column names to the column index.
		/// </summary>
		protected Dictionary<string, int> columnNames = new Dictionary<string, int>();
		/// <summary>
		/// Contains a set of IEnumerators that need to be incremented when advancing to the next line.
		/// </summary>
		protected HashSet<IEnumerator> enumerators = new HashSet<IEnumerator>();
		/// <summary>
		/// Maps from IEnumerable to IEnumerator so that passing in the same enumerable twice doesn't generate two separate Enumerators.
		/// </summary>
		Dictionary<IEnumerable, IEnumerator> enumerables = new Dictionary<IEnumerable, IEnumerator>();
		/// <summary>
		/// Maps from the column index to the column data:
		/// <list type="number">
		///		<item>
		///			<term>Enumerator</term>
		///			<description>The IEnumerator backing this column of data.</description>
		///		</item>	
		///		<item>
		///			<term>ValueExtractor</term>
		///			<description>A function that takes in an element from the IEnumerator and returns the value that this column would have based on that./></description>
		///		</item>
		///		<item>
		///			<term>Name</term>
		///			<description>The name of this column.</description>
		///		</item>
		///		<item>
		///			<term>Enumerator</term>
		///			<description>The SQL type for this column.</description>
		///		</item>
		/// </list>
		/// </summary>
		protected Dictionary<int, (IEnumerator Enumerator, Func<object, object> ValueExtractor, string Name, string Type)> columns = new Dictionary<int, (IEnumerator, Func<object, object>, string, string)>();

		/// <summary>
		/// Clears all data from this EnumeratorDataReader.
		/// </summary>
		protected virtual void Clear()
		{
			Dispose();
			columnNames = new Dictionary<string, int>();
			enumerables = new Dictionary<IEnumerable, IEnumerator>();
			enumerators = new HashSet<IEnumerator>();
			columns = new Dictionary<int, (IEnumerator Enumerator, Func<object, object> ValueExtractor, string Name, string Type)>();
		}

		/// <summary>
		/// Goes through the columns and enumerators to make sure we are storing and iterating through enumerators that aren't being used in the output.
		/// </summary>
		public virtual void CleanExcessEnumerators()
		{
			enumerators = new HashSet<IEnumerator>();

			//iterate through all enumerators in use
			foreach (var (enumerator, _, _, _) in columns.Values)
				if (enumerator != null)
					enumerators.Add(enumerator);

			//regenerate enumerables dictionary
			var tempEnumerables = new Dictionary<IEnumerable, IEnumerator>();
			foreach (var item in enumerables)
			{
				if (enumerators.Contains(item.Value))
					tempEnumerables[item.Key] = item.Value;
			}

			enumerables = tempEnumerables;
		}

		/// <summary>
		/// Adds column without worrying about adding the enumerator to the enumerator set.
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerator">The Enumerator backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerator"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		protected void AddColumn(string name, IEnumerator enumerator, Func<object, object> valueExtractor, string sqlType)
		{
			if (columnNames.TryGetValue(name, out int column))
			{
				columns[column] = (enumerator, valueExtractor, name, sqlType);
			}
			else
			{
				int row = columnNames.Count;
				columnNames[name] = row;
				columns[row] = (enumerator, valueExtractor, name, sqlType);
			}
		}

		/// <summary>
		/// Sets a column that has the same value in every row.
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string)"></seealso>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string)"></seealso>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string)"></seealso>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string)"></seealso>
		/// </summary>
		/// <param name="name">Name of the column.</param>
		/// <param name="value">The value of each row in the column.</param>
		/// <param name="sqlType">The SQL type for the column.</param>
		public virtual void SetConstant(string name, object value, string sqlType)
			=> AddColumn(name, null, _ => value, sqlType);

		/// <summary>
		/// Set a column based on <see cref="IEnumerator"/>.
		/// <seealso cref="SetConstant(string, object, string)"></seealso>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string)"></seealso>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string)"></seealso>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string)"></seealso>
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerator">The <see cref="IEnumerator"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerator"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		public virtual void Set(string name, IEnumerator enumerator, Func<object, object> valueExtractor, string sqlType)
		{
			enumerators.Add(enumerator);
			AddColumn(name, enumerator, valueExtractor, sqlType);
		}

		/// <summary>
		/// Set column based on <see cref="IEnumerable"/>.
		/// <seealso cref="SetConstant(string, object, string)"></seealso>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string)"></seealso>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string)"></seealso>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string)"></seealso>
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerable">The <see cref="IEnumerable"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerable"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		public virtual void Set(string name, IEnumerable enumerable, Func<object, object> valueExtractor, string sqlType)
		{
			if (!enumerables.TryGetValue(enumerable, out var enumerator))
			{
				enumerator = enumerable.GetEnumerator();
				enumerables[enumerable] = enumerator;
			}
			Set(name, enumerator, valueExtractor, sqlType);
		}

		/// <summary>
		/// Set a column based on <see cref="IEnumerator{T}"></see> in a somewhat type safe way.
		/// <seealso cref="SetConstant(string, object, string)"></seealso>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string)"></seealso>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string)"></seealso>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string)"></seealso>
		/// </summary>
		/// <typeparam name="T">The type T for <paramref name="enumerator"/>.</typeparam>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerator">The <see cref="IEnumerator{T}"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerator"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		public virtual void Set<T>(string name, IEnumerator<T> enumerator, Func<T, object> valueExtractor, string sqlType)
			=> Set(name, (IEnumerator)enumerator, x => valueExtractor((T)x), sqlType);

		/// <summary>
		/// Set a column based on <see cref="IEnumerable{T}"></see> in a somewhat type safe way.
		/// <seealso cref="SetConstant(string, object, string)"></seealso>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string)"></seealso>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string)"></seealso>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string)"></seealso>
		/// </summary>
		/// <typeparam name="T">The type T for <paramref name="enumerable"/>.</typeparam>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerable">The <see cref="IEnumerable{T}"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerable"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		public virtual void Set<T>(string name, IEnumerable<T> enumerable, Func<T, object> valueExtractor, string sqlType)
			=> Set(name, (IEnumerable)enumerable, x => valueExtractor((T)x), sqlType);

		/// <inheritdoc/>
		public virtual object this[int i] => GetValue(i);
		/// <inheritdoc/>
		public virtual object this[string name] => this[columnNames[name]];
		/// <inheritdoc/>
		public virtual int Depth => throw new NotImplementedException();
		/// <inheritdoc/>
		public virtual bool IsClosed => false;
		/// <inheritdoc/>
		public virtual int RecordsAffected => 0;
		/// <inheritdoc/>
		public virtual int FieldCount => columnNames.Count;
		/// <inheritdoc/>
		public virtual void Close() { }
		/// <inheritdoc/>
		public virtual void Dispose()
		{
			foreach (var item in enumerators)
				if (item is IDisposable disposable)
					disposable.Dispose();
		}
		/// <inheritdoc/>
		public virtual bool GetBoolean(int i) => (bool)GetValue(i);
		/// <inheritdoc/>
		public virtual byte GetByte(int i) => (byte)GetValue(i);
		/// <inheritdoc/>
		public virtual long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
		/// <inheritdoc/>
		public virtual char GetChar(int i) => (char)GetValue(i);
		/// <inheritdoc/>
		public virtual long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();
		/// <inheritdoc/>
		public virtual IDataReader GetData(int i) => throw new NotImplementedException();
		/// <inheritdoc/>
		public virtual string GetDataTypeName(int i) => columns[i].Type;
		/// <inheritdoc/>
		public virtual DateTime GetDateTime(int i) => (DateTime)GetValue(i);
		/// <inheritdoc/>
		public virtual decimal GetDecimal(int i) => (decimal)GetValue(i);
		/// <inheritdoc/>
		public virtual double GetDouble(int i) => (double)GetValue(i);
		/// <inheritdoc/>
		public virtual Type GetFieldType(int i) => GetValue(i).GetType();
		/// <inheritdoc/>
		public virtual float GetFloat(int i) => (float)GetValue(i);
		/// <inheritdoc/>
		public virtual Guid GetGuid(int i) => (Guid)GetValue(i);
		/// <inheritdoc/>
		public virtual short GetInt16(int i) => (short)GetValue(i);
		/// <inheritdoc/>
		public virtual int GetInt32(int i) => (int)GetValue(i);
		/// <inheritdoc/>
		public virtual long GetInt64(int i) => (long)GetValue(i);
		/// <inheritdoc/>
		public virtual string GetName(int i) => columns[i].Name;
		/// <inheritdoc/>
		public virtual int GetOrdinal(string name) => columnNames[name];
		/// <inheritdoc/>
		public virtual DataTable GetSchemaTable() => throw new NotImplementedException();
		/// <inheritdoc/>
		public virtual string GetString(int i) => (string)GetValue(i);
		/// <inheritdoc/>
		public virtual object GetValue(int i)
		{
			var (enumerator, extractor, _, _) = columns[i];
			return extractor(enumerator?.Current);
		}
		/// <inheritdoc/>
		public virtual int GetValues(object[] values) => throw new NotImplementedException();
		/// <inheritdoc/>
		public virtual bool IsDBNull(int i) => DBNull.Value.Equals(GetValue(i));

		//Not sure how this should be implemented, this is kind of just a place holder
		/// <inheritdoc/>
		public virtual bool NextResult() => false;
		/// <inheritdoc/>
		public virtual bool Read()
		{
			bool hasMoreToGo = true;
			foreach (var enumerator in enumerators)
				hasMoreToGo = hasMoreToGo && enumerator.MoveNext();
			return hasMoreToGo;
		}
	}
}
