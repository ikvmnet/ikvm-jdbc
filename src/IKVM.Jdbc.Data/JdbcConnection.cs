using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// ADO.NET wrapper for an underlying JDBC connection.
    /// </summary>
    public class JdbcConnection : DbConnection
    {

        internal string? _url;
        internal IReadOnlyDictionary<string, string>? _properties;
        internal java.sql.Connection? _connection;
        readonly bool _leaveOpen;
        internal JdbcTransaction? _transaction;

        Version? _jdbcVersion;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public JdbcConnection()
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="url"></param>
        /// <param name="properties"></param>
        public JdbcConnection(string url, IReadOnlyDictionary<string, string>? properties = null) :
            this()
        {
            if (url is null)
                throw new ArgumentNullException(nameof(url));

            _url = url;
            _properties = properties;
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="url"></param>
        public JdbcConnection(string url) :
            this(url, null)
        {
            if (url is null)
                throw new ArgumentNullException(nameof(url));
        }

        /// <summary>
        /// Initializes a new instance wrapping an existing JDBC connection.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="leaveOpen"></param>
        public JdbcConnection(Connection connection, bool leaveOpen = false)
        {
            if (connection is null)
                throw new ArgumentNullException(nameof(connection));

            _url = connection.getMetaData().getURL();
            _connection = connection;
            _leaveOpen = leaveOpen;
        }

        /// <summary>
        /// Gets the state of the connection.
        /// </summary>
        public override ConnectionState State => _connection == null ? ConnectionState.Closed : _connection.isClosed() ? ConnectionState.Closed : ConnectionState.Open;

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        internal string? Url => _url;

        /// <summary>
        /// Gets or sets the connection properties.
        /// </summary>
        public IReadOnlyDictionary<string, string>? Properties => _properties;

        /// <summary>
        /// Gets the JDBC version.
        /// </summary>
        public Version JdbcVersion => _jdbcVersion ??= GetJdbcVersion();

        /// <summary>
        /// Gets the JDBC version.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        Version GetJdbcVersion()
        {
            if (_connection is null)
                throw new InvalidOperationException();

            var md = _connection.getMetaData();
            return new Version(md.getJDBCMajorVersion(), md.getJDBCMinorVersion());
        }

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
#if NET
        [AllowNull]
#endif
        public override string ConnectionString
        {
            get => _url ?? _connection?.getMetaData().getURL() ?? "";
            set => SetUrl(value ?? "");
        }

        /// <summary>
        /// Sets the JDBC URL.
        /// </summary>
        /// <param name="url"></param>
        /// <exception cref="JdbcException"></exception>
        void SetUrl(string url)
        {
            if (State != ConnectionState.Closed)
                throw new JdbcException("Connection must be closed to update connection string.");

            // reset connection string
            _url = url;
            _connection = null;
        }

        /// <summary>
        /// Gets the current database.
        /// </summary>
        public override string Database => _connection != null ? _connection.getCatalog() : throw new JdbcException("Connection is not open.");

        /// <summary>
        /// Gets the current datasource.
        /// </summary>
        public override string DataSource => "";

        /// <summary>
        /// Gets the version of the database server if available.
        /// </summary>
        public override string ServerVersion => _connection != null ? _connection.getMetaData().getDatabaseProductVersion() : throw new JdbcException("Connection is not open.");

        /// <summary>
        /// Attempts to change the database.
        /// </summary>
        /// <param name="databaseName"></param>
        public override void ChangeDatabase(string databaseName)
        {
            if (State != ConnectionState.Open)
                throw new JdbcException("Connection must be open to change databases.");

            if (_connection is null)
                throw new InvalidOperationException();

            try
            {
                _connection.setCatalog(databaseName);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Closes the connection.
        /// </summary>
        /// <exception cref="JdbcException"></exception>
        public override void Close()
        {
            if (State == ConnectionState.Closed)
                return;

            if (_connection is null)
                throw new InvalidOperationException();

            try
            {
                if (_leaveOpen == false)
                    _connection.close();
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Opens the connection.
        /// </summary>
        /// <exception cref="JdbcException"></exception>
        public override void Open()
        {
            // already open
            if (_connection != null && State == ConnectionState.Open)
                return;

            // already open
            if (_connection != null && State == ConnectionState.Connecting)
                return;

            // was open at one time
            if (_connection != null)
                throw new JdbcException("Connection has already been opened.");

            // haven't configured the connection string
            if (_url == null)
                throw new JdbcException("JDBC URL has not been configured.");

            try
            {
                var props = new java.util.Properties();
                if (_properties is not null)
                    foreach (var entry in _properties)
                        props.setProperty(entry.Key, entry.Value);

                _connection = DriverManager.getConnection(_url, props);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Gets the current database transaction.
        /// </summary>
        /// <returns></returns>
        public JdbcTransaction? Transaction => _transaction;

        /// <summary>
        /// Starts a new JDBC transaction by disabling auto-commit.
        /// </summary>
        /// <param name="isolationLevel"></param>
        /// <returns></returns>
        /// <exception cref="JdbcException"></exception>
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (State != ConnectionState.Open)
                throw new JdbcException("Connection is not opened.");

            if (_transaction != null)
                throw new JdbcException("JDBC only supports a single ambient transaction.");

            try
            {
                return _transaction = new JdbcTransaction(this, isolationLevel);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Creates a command for execution against the current connection.
        /// </summary>
        /// <returns></returns>
        protected override DbCommand CreateDbCommand()
        {
            return CreateCommand();
        }

        /// <summary>
        /// Creates a command for execution against the current connection.
        /// </summary>
        /// <returns></returns>
        public new JdbcCommand CreateCommand()
        {
            return new JdbcCommand(this);
        }

    }

}
