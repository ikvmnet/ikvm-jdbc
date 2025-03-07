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

        readonly JdbcConnection connection;
        readonly Dictionary<string, Savepoint> savepoints = new();

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="isolationLevel"></param>
        internal JdbcTransaction(JdbcConnection connection, IsolationLevel isolationLevel)
        {
            this.connection = connection ?? throw new ArgumentNullException(nameof(connection));

            if (connection.connection.getAutoCommit() == false)
                throw new JdbcException("A JDBC transaction is already open.");
            else
            {
                // set isolation level
                connection.connection.setTransactionIsolation(isolationLevel switch
                {
                    IsolationLevel.Unspecified => java.sql.Connection.TRANSACTION_NONE,
                    IsolationLevel.Chaos => java.sql.Connection.TRANSACTION_NONE,
                    IsolationLevel.ReadUncommitted => java.sql.Connection.TRANSACTION_READ_UNCOMMITTED,
                    IsolationLevel.ReadCommitted => java.sql.Connection.TRANSACTION_READ_COMMITTED,
                    IsolationLevel.RepeatableRead => java.sql.Connection.TRANSACTION_REPEATABLE_READ,
                    IsolationLevel.Serializable => java.sql.Connection.TRANSACTION_SERIALIZABLE,
                    _ => throw new JdbcException("Unsupported IsolationLevel for JDBC."),
                });

                // disable auto commit
                connection.connection.setAutoCommit(false);
            }
        }

        /// <summary>
        /// Gets the connection associated to this transaction.
        /// </summary>
        protected override DbConnection DbConnection => connection;

        /// <summary>
        /// Gets the isolation level of the current transaction.
        /// </summary>
        public override IsolationLevel IsolationLevel => connection.connection.getTransactionIsolation() switch
        {
            java.sql.Connection.TRANSACTION_NONE => IsolationLevel.Unspecified,
            java.sql.Connection.TRANSACTION_READ_COMMITTED => IsolationLevel.ReadCommitted,
            java.sql.Connection.TRANSACTION_READ_UNCOMMITTED => IsolationLevel.ReadUncommitted,
            java.sql.Connection.TRANSACTION_REPEATABLE_READ => IsolationLevel.RepeatableRead,
            java.sql.Connection.TRANSACTION_SERIALIZABLE => IsolationLevel.Serializable,
            _ => throw new JdbcException($"Unrecognized transaction value.")
        };

        /// <summary>
        /// Commmits the current transaction.
        /// </summary>
        /// <exception cref="JdbcException"></exception>
        public override void Commit()
        {
            if (connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            try
            {
                connection.connection.commit();
                connection.connection.setAutoCommit(true);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Rollsback the current transaction.
        /// </summary>
        /// <exception cref="JdbcException"></exception>
        public override void Rollback()
        {
            if (connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            try
            {
                connection.connection.rollback();
                connection.connection.setAutoCommit(true);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

#if NET6_0_OR_GREATER
        #region SavePoints

        /// <inheritdoc />
        public override bool SupportsSavepoints => connection.connection.getMetaData().supportsSavepoints();

        /// <inheritdoc />
        public override void Save(string savepointName)
        {
            if (connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            try
            {
                savepoints[savepointName] = connection.connection.setSavepoint(savepointName);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override void Rollback(string savepointName)
        {
            if (connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            if (savepoints.TryGetValue(savepointName, out var savepoint) == false)
                throw new JdbcException("Unknown save point name.");

            try
            {
                connection.connection.rollback(savepoint);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override void Release(string savepointName)
        {
            if (connection.State != ConnectionState.Open)
                throw new JdbcException("Connection must be open commit a transaction.");

            if (savepoints.TryGetValue(savepointName, out var savepoint) == false)
                throw new JdbcException("Unknown save point name.");

            try
            {
                connection.connection.releaseSavepoint(savepoint);
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
