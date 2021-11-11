using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

namespace Gerk.SpecialDataReaders
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
		protected Dictionary<int, (IEnumerator Enumerator, Func<object, object> ValueExtractor, string Name, string SqlType, Type CSharpType)> columns = new Dictionary<int, (IEnumerator, Func<object, object>, string, string, Type)>();

		/// <summary>
		/// Clears all data from this EnumeratorDataReader.
		/// </summary>
		protected virtual void Clear()
		{
			Dispose();
			columnNames = new Dictionary<string, int>();
			enumerables = new Dictionary<IEnumerable, IEnumerator>();
			enumerators = new HashSet<IEnumerator>();
			columns = new Dictionary<int, (IEnumerator Enumerator, Func<object, object> ValueExtractor, string Name, string Type, Type cSharpType)>();
		}

		/// <summary>
		/// Goes through the columns and enumerators to make sure we are storing and iterating through enumerators that aren't being used in the output.
		/// </summary>
		public virtual void CleanExcessEnumerators()
		{
			enumerators = new HashSet<IEnumerator>();

			//iterate through all enumerators in use
			foreach (var (enumerator, _, _, _, _) in columns.Values)
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
		/// <param name="cSharpType">The C# <see cref="Type"/> of this column.</param>
		protected void AddColumn(string name, IEnumerator enumerator, Func<object, object> valueExtractor, string sqlType, Type cSharpType)
		{
			if (columnNames.TryGetValue(name, out int column))
			{
				columns[column] = (enumerator, valueExtractor, name, sqlType, cSharpType);
			}
			else
			{
				int row = columnNames.Count;
				columnNames[name] = row;
				columns[row] = (enumerator, valueExtractor, name, sqlType, cSharpType);
			}
		}

		/// <summary>
		/// Sets a column that has the same value in every row.
		/// </summary>
		/// <seealso cref="SetConstant{T}(string, T, string)"/>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerable{T}, Func{T, object}, string)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerator{T}, Func{T, V}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerable, Func{object, V}, string)"/>
		/// <seealso cref="Set{V}(string, IEnumerator, Func{object, V}, string)"/>
		/// <param name="name">Name of the column.</param>
		/// <param name="value">The value of each row in the column.</param>
		/// <param name="sqlType">The SQL type for the column.</param>
		/// <param name="cSharpType">The C# <see cref="Type"/> of this column.</param>
		public virtual void SetConstant(string name, object value, string sqlType, Type cSharpType)
			=> AddColumn(name, null, _ => value, sqlType, cSharpType);

		/// <summary>
		/// Sets a column that has the same value in every row.
		/// </summary>
		/// <seealso cref="SetConstant(string, object, string, Type)"/>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerable{T}, Func{T, object}, string)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerator{T}, Func{T, V}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerable, Func{object, V}, string)"/>
		/// <seealso cref="Set{V}(string, IEnumerator, Func{object, V}, string)"/>
		/// <typeparam name="T">Tye type of the column/constant.</typeparam>
		/// <param name="name">Name of the column.</param>
		/// <param name="value">The value of each row in the column.</param>
		/// <param name="sqlType">The SQL type for the column.</param>
		public virtual void SetConstant<T>(string name, T value, string sqlType)
			=> SetConstant(name, value, sqlType, typeof(T));

		/// <summary>
		/// Set a column based on <see cref="IEnumerator"/>.
		/// <seealso cref="SetConstant(string, object, string, Type)"/>
		/// <seealso cref="SetConstant{T}(string, T, string)"/>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerable{T}, Func{T, object}, string)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerator{T}, Func{T, V}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerable, Func{object, V}, string)"/>
		/// <seealso cref="Set{V}(string, IEnumerator, Func{object, V}, string)"/>
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerator">The <see cref="IEnumerator"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerator"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		/// <param name="cSharpType">The C# <see cref="Type"/> of this column.</param>
		public virtual void Set(string name, IEnumerator enumerator, Func<object, object> valueExtractor, string sqlType, Type cSharpType)
		{
			enumerators.Add(enumerator);
			AddColumn(name, enumerator, valueExtractor, sqlType, cSharpType);
		}

		/// <summary>
		/// Set column based on <see cref="IEnumerable"/>.
		/// <seealso cref="SetConstant(string, object, string, Type)"/>
		/// <seealso cref="SetConstant{T}(string, T, string)"/>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerable{T}, Func{T, object}, string)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerator{T}, Func{T, V}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerable, Func{object, V}, string)"/>
		/// <seealso cref="Set{V}(string, IEnumerator, Func{object, V}, string)"/>
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerable">The <see cref="IEnumerable"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerable"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		/// <param name="cSharpType">The C# <see cref="Type"/> of this column.</param>
		public virtual void Set(string name, IEnumerable enumerable, Func<object, object> valueExtractor, string sqlType, Type cSharpType)
		{
			if (!enumerables.TryGetValue(enumerable, out var enumerator))
			{
				enumerator = enumerable.GetEnumerator();
				enumerables[enumerable] = enumerator;
			}
			Set(name, enumerator, valueExtractor, sqlType, cSharpType);
		}

		/// <summary>
		/// Set a column based on <see cref="IEnumerator"/>.
		/// <seealso cref="SetConstant(string, object, string, Type)"/>
		/// <seealso cref="SetConstant{T}(string, T, string)"/>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerable{T}, Func{T, object}, string)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerator{T}, Func{T, V}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerable, Func{object, V}, string)"/>
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerator">The <see cref="IEnumerator"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerator"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		public virtual void Set<V>(string name, IEnumerator enumerator, Func<object, V> valueExtractor, string sqlType)
		{
			enumerators.Add(enumerator);
			AddColumn(name, enumerator, x => valueExtractor(x), sqlType, typeof(V));
		}

		/// <summary>
		/// Set column based on <see cref="IEnumerable"/>.
		/// <seealso cref="SetConstant(string, object, string, Type)"/>
		/// <seealso cref="SetConstant{T}(string, T, string)"/>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerable{T}, Func{T, object}, string)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerator{T}, Func{T, V}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerator, Func{object, V}, string)"/>
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerable">The <see cref="IEnumerable"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerable"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		public virtual void Set<V>(string name, IEnumerable enumerable, Func<object, V> valueExtractor, string sqlType)
		{
			if (!enumerables.TryGetValue(enumerable, out var enumerator))
			{
				enumerator = enumerable.GetEnumerator();
				enumerables[enumerable] = enumerator;
			}
			Set(name, enumerator, x => valueExtractor(x), sqlType, typeof(V));
		}

		/// <summary>
		/// Set a column based on <see cref="IEnumerator{T}"></see> in a somewhat type safe way.
		/// <seealso cref="SetConstant(string, object, string, Type)"/>
		/// <seealso cref="SetConstant{T}(string, T, string)"/>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerable{T}, Func{T, object}, string)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerator{T}, Func{T, V}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerable, Func{object, V}, string)"/>
		/// <seealso cref="Set{V}(string, IEnumerator, Func{object, V}, string)"/>
		/// </summary>
		/// <typeparam name="T">The type T for <paramref name="enumerator"/>.</typeparam>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerator">The <see cref="IEnumerator{T}"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerator"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		/// <param name="cSharpType">The C# <see cref="Type"/> of this column.</param>
		public virtual void Set<T>(string name, IEnumerator<T> enumerator, Func<T, object> valueExtractor, string sqlType, Type cSharpType)
			=> Set(name, (IEnumerator)enumerator, x => valueExtractor((T)x), sqlType, cSharpType);

		/// <summary>
		/// Set a column based on <see cref="IEnumerable{T}"></see> in a somewhat type safe way.
		/// <seealso cref="SetConstant(string, object, string, Type)"/>
		/// <seealso cref="SetConstant{T}(string, T, string)"/>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerable{T}, Func{T, object}, string)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerator{T}, Func{T, V}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerable, Func{object, V}, string)"/>
		/// <seealso cref="Set{V}(string, IEnumerator, Func{object, V}, string)"/>
		/// </summary>
		/// <typeparam name="T">The type T for <paramref name="enumerable"/>.</typeparam>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerable">The <see cref="IEnumerable{T}"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerable"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		/// <param name="cSharpType">The C# <see cref="Type"/> of this column.</param>
		public virtual void Set<T>(string name, IEnumerable<T> enumerable, Func<T, object> valueExtractor, string sqlType, Type cSharpType)
			=> Set(name, (IEnumerable)enumerable, x => valueExtractor((T)x), sqlType, cSharpType);

		/// <summary>
		/// Set a column based on <see cref="IEnumerator{T}"></see> in a fairly type safe way.
		/// <seealso cref="SetConstant(string, object, string, Type)"/>
		/// <seealso cref="SetConstant{T}(string, T, string)"/>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerable{T}, Func{T, object}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerable, Func{object, V}, string)"/>
		/// <seealso cref="Set{V}(string, IEnumerator, Func{object, V}, string)"/>
		/// </summary>
		/// <typeparam name="T">The type T for <paramref name="enumerator"/>.</typeparam>
		/// <typeparam name="V">Th type of the column that <paramref name="valueExtractor"/> maps to.</typeparam>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerator">The <see cref="IEnumerator{T}"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerator"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		public virtual void Set<T, V>(string name, IEnumerator<T> enumerator, Func<T, V> valueExtractor, string sqlType)
			=> Set(name, (IEnumerator)enumerator, x => valueExtractor((T)x), sqlType, typeof(V));

		/// <summary>
		/// Set a column based on <see cref="IEnumerable{T}"></see> in a fairly type safe way.
		/// <seealso cref="SetConstant(string, object, string, Type)"/>
		/// <seealso cref="SetConstant{T}(string, T, string)"/>
		/// <seealso cref="Set(string, IEnumerable, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set(string, IEnumerator, Func{object, object}, string, Type)"/>
		/// <seealso cref="Set{T, V}(string, IEnumerator{T}, Func{T, V}, string)"/>
		/// <seealso cref="Set{T}(string, IEnumerable{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{T}(string, IEnumerator{T}, Func{T, object}, string, Type)"/>
		/// <seealso cref="Set{V}(string, IEnumerable, Func{object, V}, string)"/>
		/// <seealso cref="Set{V}(string, IEnumerator, Func{object, V}, string)"/>
		/// </summary>
		/// <typeparam name="T">The type T for <paramref name="enumerable"/>.</typeparam>
		/// <typeparam name="V">Th type of the column that <paramref name="valueExtractor"/> maps to.</typeparam>
		/// <param name="name">The name of the column.</param>
		/// <param name="enumerable">The <see cref="IEnumerable{T}"/> backing the column.</param>
		/// <param name="valueExtractor">Function mapping from element of <paramref name="enumerable"/> to corresponding column value.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		public virtual void Set<T, V>(string name, IEnumerable<T> enumerable, Func<T, object> valueExtractor, string sqlType)
			=> Set(name, (IEnumerable)enumerable, x => valueExtractor((T)x), sqlType, typeof(V));

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
		public virtual string GetDataTypeName(int i) => columns[i].SqlType;
		/// <inheritdoc/>
		public virtual DateTime GetDateTime(int i) => (DateTime)GetValue(i);
		/// <inheritdoc/>
		public virtual decimal GetDecimal(int i) => (decimal)GetValue(i);
		/// <inheritdoc/>
		public virtual double GetDouble(int i) => (double)GetValue(i);
		/// <inheritdoc/>
		public virtual Type GetFieldType(int i) => columns[i].CSharpType;
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
			var (enumerator, extractor, _, _, _) = columns[i];
			return extractor(enumerator?.Current);
		}
		/// <inheritdoc/>
		public virtual int GetValues(object[] values)
		{
			int i;
			int len = Math.Min(values.Length, FieldCount);
			for (i = 0; i < len; i++)
				values[i] = GetValue(i);
			return i;
		}
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
