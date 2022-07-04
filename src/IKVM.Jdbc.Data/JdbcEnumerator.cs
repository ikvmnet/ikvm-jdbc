using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Supports a simple iteration over a collection by the JDBC data provider.
    /// </summary>
    class JdbcEnumerator : DbEnumerator, IEnumerator<IDataRecord>
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="reader"></param>
        public JdbcEnumerator(JdbcDataReaderBase reader) :
            base(reader)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="closeReader"></param>
        public JdbcEnumerator(JdbcDataReaderBase reader, bool closeReader) :
            base(reader, closeReader)
        {

        }

        /// <summary>
        /// Gets the current element in the collection.
        /// </summary>
        public new IDataRecord Current => (IDataRecord)base.Current;

        /// <summary>
        /// Disposes of the instance.
        /// </summary>
        public void Dispose()
        {

        }

    }

}
