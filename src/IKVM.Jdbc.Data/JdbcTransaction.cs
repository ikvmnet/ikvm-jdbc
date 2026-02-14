using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Manages a transaction over a <see cref="JdbcConnection"/>.
    /// </summary>
    public class JdbcTransaction : DbTransaction
    {

        /// <summary>
        /// Gets a JDBC isolation level for the specified ADO.NET isolation level.
        /// </summary>
        /// <param name="level"></param>
        /// <returns></returns>
        /// <exception cref="JdbcException"></exception>
        static int GetJdbcIsolationLevel(IsolationLevel level) => level switch
        {
            IsolationLevel.Unspecified => java.sql.Connection.TRANSACTION_NONE,
            IsolationLevel.Chaos => java.sql.Connection.TRANSACTION_NONE,
            IsolationLevel.ReadUncommitted => java.sql.Connection.TRANSACTION_READ_UNCOMMITTED,
            IsolationLevel.ReadCommitted => java.sql.Connection.TRANSACTION_READ_COMMITTED,
            IsolationLevel.RepeatableRead => java.sql.Connection.TRANSACTION_REPEATABLE_READ,
            IsolationLevel.Serializable => java.sql.Connection.TRANSACTION_SERIALIZABLE,
            _ => throw new JdbcException("Unsupported IsolationLevel for JDBC."),
        };

        readonly JdbcConnection _connection;
        readonly Dictionary<string, Savepoint> _savepoints = new();

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="isolationLevel"></param>
        internal JdbcTransaction(JdbcConnection connection, IsolationLevel isolationLevel)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));

            var jdbcConnection = _connection.JdbcConnection;

            // check whether isolation level is supported
            var level = GetJdbcIsolationLevel(isolationLevel);
            if (level != java.sql.Connection.TRANSACTION_NONE && jdbcConnection.getMetaData().supportsTransactionIsolationLevel(level) == false)
                throw new JdbcException("JDBC driver does not support the specified IsolationLevel.");

            // set isolation level
            jdbcConnection.setTransactionIsolation(level);

            // disable auto commit
            jdbcConnection.setAutoCommit(false);
        }

        /// <summary>
        /// Gets the connection associated to this transaction.
        /// </summary>
        protected override DbConnection DbConnection => _connection;

        /// <inheritdoc />
        public new JdbcConnection Connection => _connection;

        /// <summary>
        /// Gets the isolation level of the current transaction.
        /// </summary>
        public override IsolationLevel IsolationLevel => _connection._connection?.getTransactionIsolation() switch
        {
            java.sql.Connection.TRANSACTION_NONE => IsolationLevel.Unspecified,
            java.sql.Connection.TRANSACTION_READ_COMMITTED => IsolationLevel.ReadCommitted,
            java.sql.Connection.TRANSACTION_READ_UNCOMMITTED => IsolationLevel.ReadUncommitted,
            java.sql.Connection.TRANSACTION_REPEATABLE_READ => IsolationLevel.RepeatableRead,
            java.sql.Connection.TRANSACTION_SERIALIZABLE => IsolationLevel.Serializable,
            null => throw new InvalidOperationException(),
            _ => throw new JdbcException($"Unrecognized transaction value.")
        };

        /// <summary>
        /// Commmits the current transaction.
        /// </summary>
        /// <exception cref="JdbcException"></exception>
        public override void Commit()
        {
            if (_connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            if (_connection._connection is null)
                throw new InvalidOperationException();

            try
            {
                _connection._connection.commit();
                _connection._connection.setAutoCommit(true);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Rolls back the current transaction.
        /// </summary>
        /// <exception cref="JdbcException"></exception>
        public override void Rollback()
        {
            if (_connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            if (_connection._connection is null)
                throw new InvalidOperationException();

            try
            {
                _connection._connection.rollback();
                _connection._connection.setAutoCommit(true);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

#if NET6_0_OR_GREATER
        #region SavePoints

        /// <inheritdoc />
        public override bool SupportsSavepoints => _connection._connection?.getMetaData()?.supportsSavepoints() ?? throw new InvalidOperationException();

        /// <inheritdoc />
        public override void Save(string savepointName)
        {
            if (_connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            if (_connection._connection is null)
                throw new InvalidOperationException();

            try
            {
                _savepoints[savepointName] = _connection._connection.setSavepoint(savepointName);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override void Rollback(string savepointName)
        {
            if (_connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            if (_savepoints.TryGetValue(savepointName, out var savepoint) == false)
                throw new JdbcException("Unknown save point name.");

            if (_connection._connection is null)
                throw new InvalidOperationException();

            try
            {
                _connection._connection.rollback(savepoint);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override void Release(string savepointName)
        {
            if (_connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            if (_savepoints.TryGetValue(savepointName, out var savepoint) == false)
                throw new JdbcException("Unknown save point name.");

            if (_connection._connection is null)
                throw new InvalidOperationException();

            try
            {
                _connection._connection.releaseSavepoint(savepoint);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        #endregion
#endif

    }

}
