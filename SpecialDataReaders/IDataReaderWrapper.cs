using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Gerk.SpecialDataReaders
{
	/// <summary>
	/// Interface for IDataReader wrapping another data reader and wanting to expose the underlying.
	/// </summary>
	/// <typeparam name="T">The specific type of IDataReader underlying.</typeparam>
	public interface IDataReaderWrapper<T> : IDataReader where T : IDataReader
	{
		/// <summary>
		/// Gets a reference to the underlying data reader.
		/// </summary>
		T UnderlyingDataReader { get; }
	}
}
