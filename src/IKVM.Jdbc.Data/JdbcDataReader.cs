using System.Collections.Generic;
using System.Data;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    public class JdbcDataReader : JdbcDataReaderBase, IEnumerable<IDataRecord>
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="command"></param>
        /// <param name="statement"></param>
        /// <param name="getGeneratedKeys"></param>
        internal JdbcDataReader(JdbcCommand command, Statement statement, bool getGeneratedKeys) :
            base(command, statement, getGeneratedKeys)
        {

        }

        /// <summary>
        /// Gets an enumerator that can be used to iterate through the rows.
        /// </summary>
        /// <returns></returns>
        public new IEnumerator<IDataRecord> GetEnumerator()
        {
            return (JdbcEnumerator)base.GetEnumerator();
        }

    }

}
