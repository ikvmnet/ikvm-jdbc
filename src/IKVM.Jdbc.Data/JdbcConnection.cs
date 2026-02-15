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
    public class JdbcConnection : JdbcConnectionBase
    {

        /// <summary>
        /// Data to open from a URL.
        /// </summary>
        (string? Url, IReadOnlyDictionary<string, string>? Properties)? _openFromUrl;

        /// <summary>
        /// Function to open a new connection with.
        /// </summary>
        Func<Connection>? _openFromFunc;

        /// <summary>
        /// Open based on a single existing connection.
        /// </summary>
        (Connection Connection, bool LeaveOpen)? _openFromConnection;

        internal Connection? _connection;
        internal bool _leaveOpen = false;
        internal JdbcTransaction? _transaction;

        Version? _jdbcVersion;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public JdbcConnection()
        {

        }

        /// <summary>
        /// Initializes a new instance from a URL and properties.
        /// </summary>
        /// <param name="url"></param>
        /// <param name="properties"></param>
        public JdbcConnection(string url, IReadOnlyDictionary<string, string>? properties = null) :
            this()
        {
            if (url is null)
                throw new ArgumentNullException(nameof(url));

            _openFromUrl = (url, properties);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="url"></param>
        public JdbcConnection(string url) :
            this(url, null)
        {

        }

        /// <summary>
        /// Initializes a new instance that opens from a function that delivers a connection.
        /// </summary>
        /// <param name="openFunc"></param>
        public JdbcConnection(Func<Connection> openFunc)
        {
            if (openFunc is null)
                throw new ArgumentNullException(nameof(openFunc));

            _openFromFunc = openFunc;
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

            _openFromConnection = (connection, leaveOpen);
        }

        /// <summary>
        /// Gets the state of the connection.
        /// </summary>
        public override ConnectionState State => _connection != null && _connection.isClosed() == false ? ConnectionState.Open : ConnectionState.Closed;

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        internal string? Url => _openFromUrl.HasValue ? _openFromUrl.Value.Url : _connection?.getMetaData().getURL();

        /// <summary>
        /// Gets or sets the connection properties.
        /// </summary>
        public IReadOnlyDictionary<string, string>? Properties => _openFromUrl.HasValue ? _openFromUrl.Value.Properties : null;

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
                throw new JdbcException("JDBC connection is not yet constructed.");

            var md = _connection.getMetaData();
            return new Version(md.getJDBCMajorVersion(), md.getJDBCMinorVersion());
        }

        /// <summary>
        /// Gets the JDBC connection.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        protected override Connection GetJdbcConnection()
        {
            if (_connection is null)
                throw new JdbcException("JDBC connection is not yet constructed.");

            return _connection;
        }

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
#if NET
        [AllowNull]
#endif
        public override string ConnectionString
        {
            get => Url ?? "";
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

            // reset to open from URL
            _openFromUrl = (url, _openFromUrl?.Properties);
            _openFromFunc = null;
            _openFromConnection = null;
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
                {
                    _connection.close();
                    _connection = null;
                }
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
            if (State == ConnectionState.Open)
                throw new InvalidOperationException("Connection is already open.");

            try
            {
                // we have a URL to open with
                if (_openFromUrl.HasValue)
                {
                    var props = new java.util.Properties();
                    if (_openFromUrl.Value.Properties is not null)
                        foreach (var entry in _openFromUrl.Value.Properties)
                            props.setProperty(entry.Key, entry.Value);

                    _connection = DriverManager.getConnection(_openFromUrl.Value.Url, props);
                    _leaveOpen = false;
                    return;
                }

                // we have a function to open with
                if (_openFromFunc != null)
                {
                    _connection = _openFromFunc();
                    _leaveOpen = false;
                    return;
                }

                // we open from a single connection
                if (_openFromConnection.HasValue)
                {
                    _connection = _openFromConnection.Value.Connection;
                    _leaveOpen = _openFromConnection.Value.LeaveOpen;
                    _openFromConnection = null;
                    return;
                }

                throw new JdbcException("Cannot open a connection. No remaining methods to open.");
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

            if (JdbcConnection.getMetaData().supportsTransactions() == false)
                throw new JdbcException("JDBC driver does not support transactions.");

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
