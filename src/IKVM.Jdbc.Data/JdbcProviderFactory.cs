using System;
using System.Data.Common;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Provides a set of methods for creating instances of the JDBC database provider classes.
    /// </summary>
    public class JdbcProviderFactory : DbProviderFactory
    {

        /// <summary>
        /// Gets the default instance.
        /// </summary>
        public static JdbcProviderFactory Instance { get; } = new JdbcProviderFactory();

        /// <inheritdoc />
        public override DbCommand CreateCommand()
        {
            return new JdbcCommand();
        }

        /// <inheritdoc />
        public override DbCommandBuilder CreateCommandBuilder()
        {
            return new JdbcCommandBuilder();
        }

        /// <inheritdoc />
        public override DbConnection CreateConnection()
        {
            return new JdbcConnection();
        }

        /// <inheritdoc />
        public override DbConnectionStringBuilder CreateConnectionStringBuilder()
        {
            throw new NotSupportedException(); 
        }

        /// <inheritdoc />
        public override DbDataAdapter CreateDataAdapter()
        {
            return new JdbcDataAdapter();
        }

        /// <inheritdoc />
        public override DbParameter CreateParameter()
        {
            return new JdbcParameter();
        }

    }

}
