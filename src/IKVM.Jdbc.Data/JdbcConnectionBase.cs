using System.Data.Common;

namespace IKVM.Jdbc.Data
{

    public abstract class JdbcConnectionBase : DbConnection
    {

        /// <summary>
        /// Gets the JDBC connection.
        /// </summary>
        public java.sql.Connection JdbcConnection => GetJdbcConnection();

        /// <summary>
        /// Gets the JDBC connection.
        /// </summary>
        /// <returns></returns>
        protected abstract java.sql.Connection GetJdbcConnection();

    }

}