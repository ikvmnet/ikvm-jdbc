using System.Data.Common;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Provides a set of methods for creating instances of the JDBC database provider classes.
    /// </summary>
    public class JdbcProviderFactory : DbProviderFactory
    {

        /// <summary>
        /// Returns a new instance of <see cref="JdbcCommand"/>.
        /// </summary>
        /// <returns></returns>
        public override DbCommand CreateCommand()
        {
            return new JdbcCommand();
        }

        /// <summary>
        /// Returns a new instance of <see cref="JdbcCommandBuilder"/>.
        /// </summary>
        /// <returns></returns>
        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new JdbcCommandBuilder();
        }

        /// <summary>
        /// Returns a new instance of <see cref="JdbcConnection"/>.
        /// </summary>
        /// <returns></returns>
        public override DbConnection CreateConnection()
        {
            return new JdbcConnection();
        }

        /// <summary>
        /// Returns a new instance of <see cref="JdbcConnectionStringBuilder"/>.
        /// </summary>
        /// <returns></returns>
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            return new JdbcConnectionStringBuilder();
        }

        /// <summary>
        /// Returns a new instance of <see cref="JdbcDataAdapter"/>.
        /// </summary>
        /// <returns></returns>
        public override DbDataAdapter CreateDataAdapter()
        {
            return new JdbcDataAdapter();
        }

        /// <summary>
        /// Returns a new instance of <see cref="JdbcParameter"/>.
        /// </summary>
        /// <returns></returns>
        public override DbParameter CreateParameter()
        {
            return new JdbcParameter();
        }

    }

}
