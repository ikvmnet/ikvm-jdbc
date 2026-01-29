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

        readonly string _url;
        internal IReadOnlyDictionary<string, string>? _properties;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="url"></param>
        /// <param name="properties"></param>
        public JdbcDataSource(string url) :
            this(url, new Dictionary<string, string>())
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="url"></param>
        /// <param name="properties"></param>
        public JdbcDataSource(string url, IReadOnlyDictionary<string, string> properties)
        {
            ArgumentNullException.ThrowIfNull(url);
            ArgumentNullException.ThrowIfNull(properties);

            _url = url;
            _properties = properties;
        }

        /// <inheritdoc />
        public override string ConnectionString => _url;

        /// <inheritdoc />
        protected override DbConnection CreateDbConnection()
        {
            return new JdbcConnection(_url, _properties);
        }

    }

}

#endif
