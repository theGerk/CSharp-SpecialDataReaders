using System;
using System.Data;

namespace Gerk.SpecialDataReaders
{
	/// <summary>
	/// This Implementation is meant to take already existing data in an <see cref="IDataReader"></see> and add additional columns.
	/// </summary>
	/// <typeparam name="T">The IDataReader Implementation that underlies the data</typeparam>
	public class DataReaderExtender<T> : EnumeratorDataReader where T : IDataReader
	{
		/// <summary>
		/// The underlying <see cref="IDataReader"/> that is having columns added.
		/// </summary>
		private readonly T data;

		/// <summary>
		/// Constructor taking in an <see cref="IDataReader"/> as underlying data.
		/// </summary>
		/// <param name="underlyingData">The initial data.</param>
		public DataReaderExtender(T underlyingData)
		{
			data = underlyingData;
			Initialize();
		}

		/// <summary>
		/// Adds column that is computed based of a function.
		/// </summary>
		/// <param name="name">The name of the column.</param>
		/// <param name="valueExtractor">Extracts a value from the underlying data reader.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		/// <param name="cSharpType">The C# <see cref="Type"/> of this column.</param>
		public virtual void Set(string name, Func<T, object> valueExtractor, string sqlType, Type cSharpType)
			=> AddColumn(name, null, _ => valueExtractor(data), sqlType, cSharpType);

		/// <summary>
		/// Adds column that is computed based of a function.
		/// </summary>
		/// <typeparam name="V">Th type of the column that <paramref name="valueExtractor"/> maps to.</typeparam>
		/// <param name="name">The name of the column.</param>
		/// <param name="valueExtractor">Extracts a value from the underlying data reader.</param>
		/// <param name="sqlType">The columns SQL type.</param>
		public virtual void Set<V>(string name, Func<T, V> valueExtractor, string sqlType)
			=> AddColumn(name, null, _ => valueExtractor(data), sqlType, typeof(V));

		/// <summary>
		/// Clears out all added data. The <see cref="data"></see> is left alone.
		/// </summary>
		protected override void Clear()
		{
			base.Clear();
			Initialize();
		}

		/// <summary>
		/// Sets up <see cref="EnumeratorDataReader.columnNames"></see> field with the columns from <see cref="data"></see>.
		/// </summary>
		private void Initialize()
		{
			for (var i = data.FieldCount - 1; i >= 0; i--)
				columnNames[data.GetName(i)] = i;
		}

		/// <inheritdoc/>
		public override string GetDataTypeName(int i)
		{
			if (columns.TryGetValue(i, out var dat))
				return dat.SqlType;
			else
				return data.GetDataTypeName(i);
		}

		/// <inheritdoc/>
		public override Type GetFieldType(int i)
		{
			if (columns.ContainsKey(i))
				return base.GetFieldType(i);
			else
				return data.GetFieldType(i);
		}

		/// <inheritdoc/>
		public override string GetName(int i)
		{
			if (columns.ContainsKey(i))
				return base.GetName(i);
			else
				return data.GetName(i);
		}

		/// <inheritdoc/>
		public override object GetValue(int i)
		{
			if (columns.ContainsKey(i))
				return base.GetValue(i);
			else
				return data.GetValue(i);
		}


		/// <summary>
		/// Goes to next result in the underlying data. Clears out all added columns.
		/// </summary>
		/// <returns>If there is another result. <see langword="false"/> means there are none left.</returns>
		public override bool NextResult()
		{
			var output = data.NextResult();
			if (output)
				Clear();
			return output;
		}

		/// <inheritdoc/>
		public override bool Read()
		{
			//Note: The commented line of code below would not work as a "prettier" version of this function as short circuiting could cause undesirable results.
			//return data.Read() && base.Read();
			var dataReturn = data.Read();
			var baseReturn = base.Read();
			return dataReturn && baseReturn;
		}
	}
}
