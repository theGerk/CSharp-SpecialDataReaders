using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Gerk.SpecialDataReaders
{
	/// <summary>
	/// Generic SQL logic
	/// </summary>
	public static class GenericLogic
	{
		//not sure if this stuff should be public, but its useful in a bunch of places.
		#region Handling DB Nulls
		/// <summary>
		/// Takes an object that may be a <see cref="DBNull.Value"/> and turns it into regular C# <see langword="null"/> if it is.
		/// </summary>
		/// <seealso cref="NullToDBNull(object)"/>
		/// <param name="input">Input object that may be <see cref="DBNull.Value"/>.</param>
		/// <returns>The input object if it is not <see cref="DBNull.Value"/>, and <see langword="null"/> otherwise.</returns>
		public static object DBNullToNull(this object input)
		{
			if (DBNull.Value.Equals(input))
				return null;
			else
				return input;
		}
		/// <summary>
		/// Takes an object that may be a <see langword="null"/> and turns it into <see cref="DBNull.Value"/> for use with a database.
		/// </summary>
		/// <seealso cref="DBNullToNull(object)"/>
		/// <param name="input">Input object that may be <see langword="null"/>.</param>
		/// <returns>The input object if it is not <see langword="null"/>, and <see cref="DBNull.Value"/> otherwise.</returns>
		public static object NullToDBNull(this object input) => input ?? DBNull.Value;
		#endregion
	}
}
