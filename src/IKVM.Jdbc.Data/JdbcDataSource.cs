#if NET8_0_OR_GREATER

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using java.sql;

namespace IKVM.Jdbc.Data
{

    public class JdbcDataSource : DbDataSource
    {

        readonly string _connectionString;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connectionString"></param>
        public JdbcDataSource(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="url"></param>
        /// <param name="properties"></param>
        public JdbcDataSource(string url, IDictionary<string, string> properties)
        {
            ArgumentNullException.ThrowIfNull(url);
            ArgumentNullException.ThrowIfNull(properties);

            var _connectionStringBuilder = new JdbcConnectionStringBuilder() { Url = url };

            // fill with properties
            foreach (var kvp in properties)
                _connectionStringBuilder.Add(kvp.Key, kvp.Value);

            // convert to normal connection string
            _connectionString = _connectionStringBuilder.ConnectionString;
        }

        /// <inheritdoc />
        public override string ConnectionString => _connectionString;

        /// <inheritdoc />
        protected override DbConnection CreateDbConnection()
        {
            return new JdbcConnection(_connectionString);
        }

    }

}

#endif
