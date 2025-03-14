using System;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Represents a SQL statement or stored procedure to execute against a <see cref="JdbcConnection"/>.
    /// </summary>
    public abstract partial class JdbcCommandBase : DbCommand
    {

        /// <summary>
        /// Manages locks around execution.
        /// </summary>
        readonly struct ExecutingLock : IDisposable
        {

            readonly JdbcCommandBase _command;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="command"></param>
            /// <exception cref="ArgumentNullException"></exception>
            public ExecutingLock(JdbcCommandBase command)
            {
                _command = command ?? throw new ArgumentNullException(nameof(command));
                Monitor.Enter(_command._syncRoot);

                if (_command._executing != null)
                    throw new JdbcException("The command is already executing a statement.");
            }

            /// <summary>
            /// Exits the lock.
            /// </summary>
            public void Exit()
            {
                Monitor.Exit(_command._syncRoot);
            }

            /// <summary>
            /// Enters the lock an additional time.
            /// </summary>
            public void Enter()
            {
                Monitor.Enter(_command._syncRoot);
            }

            /// <summary>
            /// Releases the lock.
            /// </summary>
            public void Dispose()
            {
                Monitor.Exit(_command._syncRoot);
            }

        }

        readonly object _syncRoot = new object();
        readonly JdbcParameterCollection _parameters = new JdbcParameterCollection();
        JdbcConnection? _connection;
        CommandType _commandType = CommandType.Text;
        string? _commandText;
        PreparedStatement? _prepared;
        Statement? _executing;

        /// <summary>
        /// Creates a new command.
        /// </summary>
        internal JdbcCommandBase()
        {

        }

        /// <summary>
        /// Creates a new command attached to the specified connection.
        /// </summary>
        /// <param name="connection"></param>
        internal JdbcCommandBase(JdbcConnection connection)
        {
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        }

        /// <summary>
        /// Gets or sets the database connection.
        /// </summary>
        protected override DbConnection? DbConnection
        {
            get => _connection;
            set
            {
                using (new ExecutingLock(this))
                    _connection = (JdbcConnection?)value;
            }
        }

        /// <summary>
        /// Gets or sets how the <see cref="CommandText"/> property is interpreted.
        /// </summary>
        public override CommandType CommandType
        {
            get => _commandType;
            set
            {
                using (new ExecutingLock(this))
                {
                    _prepared = null;
                    _commandType = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the text command to run against the data source.
        /// </summary>
#if NET
        [AllowNull]
#endif
        public override string CommandText
        {
            get => _commandText ?? "";
            set
            {
                using (new ExecutingLock(this))
                {
                    _prepared = null;
                    _commandText = value;
                }
            }
        }

        /// <summary>
        /// Gets or sets the wait time (in seconds) before terminating the attempt to execute the command and generating an error.
        /// </summary>
        public override int CommandTimeout { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the command object should be visible in a customized interface control.
        /// </summary>
        public override bool DesignTimeVisible { get; set; } = false;

        /// <summary>
        /// Gets or sets how command results are applied to the <see cref="DataRow"/> when used by the Update method of a <see cref="DbDataAdapter"/>.
        /// </summary>
        public override UpdateRowSource UpdatedRowSource { get; set; }

        /// <summary>
        /// Gets the collection of <see cref="DbParameter"/> objects.
        /// </summary>
        protected override DbParameterCollection DbParameterCollection => _parameters;

        /// <summary>
        /// Gets or sets the <see cref="DbTransaction"/> within which this <see cref="JdbcCommandBase"/> object executes.
        /// </summary>
        protected override DbTransaction? DbTransaction { get; set; }

        /// <summary>
        /// Indicates that the JDBC <see cref="Statement.RETURN_GENERATED_KEYS" /> value will be used to retrieve an extra result set.
        /// </summary>
        public bool ReturnGeneratedKeys { get; set; }

        /// <summary>
        /// Creates a new instance of a <see cref="DbParameter"/> object.
        /// </summary>
        /// <returns></returns>
        protected override DbParameter CreateDbParameter()
        {
            if (_prepared != null)
                throw new JdbcException("Command is already prepared.");

            return new JdbcParameter();
        }

        /// <summary>
        /// Extracts any JDBC command extension flags from the command text.
        /// </summary>
        /// <param name="text"></param>
        /// <param name="flag"></param>
        /// <returns></returns>
        string ExtractCommandExtension(string text, out JdbcCommandExtensionFlag flag)
        {
            flag = JdbcCommandExtensionFlag.None;
            text = text.TrimEnd();
            var getGeneratedKeysText = "-- :GetGeneratedKeys";
            if (text.EndsWith(getGeneratedKeysText, StringComparison.OrdinalIgnoreCase))
            {
                flag |= JdbcCommandExtensionFlag.GetGeneratedKeys;
                text = text.Remove(text.Length - getGeneratedKeysText.Length, getGeneratedKeysText.Length);
            }

            // can be set explicitly on the command
            if (ReturnGeneratedKeys)
                flag |= JdbcCommandExtensionFlag.GetGeneratedKeys;

            return text;
        }

        /// <summary>
        /// Builds the JDBC escape string for a stored procedure call.
        /// </summary>
        /// <returns></returns>
        string? BuildJdbcStoredProcedureCallString(string text)
        {
            var b = new StringBuilder("{ ");

            if (_parameters.Any(i => i.Direction == ParameterDirection.ReturnValue))
                b.Append("? = ");

            b.Append("call ");
            b.Append(text);
            b.Append("(");
            for (int i = 0; i < _parameters.Count; i++)
            {
                b.Append("?");
                if (i < _parameters.Count - 1)
                    b.Append(", ");
            }
            b.Append(")");
            b.Append(" }");

            return b.ToString();
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        public override void Prepare()
        {
            var type = CommandType;
            var text = ExtractCommandExtension(CommandText, out var flags);

            // always prepare if we're going to use parameters
            var hasAutoGeneratedKeys = (flags & JdbcCommandExtensionFlag.GetGeneratedKeys) != 0;
            Prepare(type, text, hasAutoGeneratedKeys);
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        void Prepare(CommandType type, string text, bool hasAutoGeneratedKeys)
        {
            using (new ExecutingLock(this))
            {
                if (_connection == null)
                    throw new JdbcException("Connection must be available.");

                if (_connection.State != ConnectionState.Open)
                    throw new JdbcException("Connection must be open.");

                if (_prepared != null)
                    throw new JdbcException("Command is already prepared.");

                if (_connection._connection is null)
                    throw new InvalidOperationException();

                try
                {
                    switch (type)
                    {
                        case CommandType.Text:
                            if (hasAutoGeneratedKeys)
                                _prepared = _connection._connection.prepareStatement(text, Statement.RETURN_GENERATED_KEYS);
                            else
                                _prepared = _connection._connection.prepareStatement(text);
                            break;
                        case CommandType.StoredProcedure:
                            if (hasAutoGeneratedKeys)
                                throw new JdbcException("GetGeneratedKeys not supported when executing Stored Procedures.");
                            else
                                _prepared = _connection._connection.prepareCall(BuildJdbcStoredProcedureCallString(text));
                            break;
                        case CommandType.TableDirect:
                            throw new NotImplementedException();
                    }
                }
                catch (SQLException e)
                {
                    throw new JdbcException(e);
                }
            }
        }

        /// <summary>
        /// Sets the value of the parameter on the given statement.
        /// </summary>
        /// <param name="statement"></param>
        /// <param name="parameter"></param>
        void ApplyParameterValue(PreparedStatement statement, JdbcParameter parameter, int offset)
        {
            if (statement is null)
                throw new ArgumentNullException(nameof(statement));
            if (parameter is null)
                throw new ArgumentNullException(nameof(parameter));

            if (int.TryParse(parameter.ParameterName, out var index) == false || index < 0)
                throw new JdbcException("Parameter names must be postive integers.");

            // if parameter is to be used as input, set the value based on the index
            if (parameter.Direction == ParameterDirection.Input || parameter.Direction == ParameterDirection.InputOutput)
            {
                statement.SetParameterValue(index + offset, parameter.DbType, parameter.Value);
                return;
            }

            // if parameter is to be used as output, set the value based on the index
            if (parameter.Direction == ParameterDirection.Output || parameter.Direction == ParameterDirection.InputOutput || parameter.Direction == ParameterDirection.ReturnValue)
            {
                if (statement is not CallableStatement callableStatement)
                    throw new JdbcException("Cannot add output statement to non-callable statement.");

                callableStatement.registerOutParameter(index + offset, JdbcDbType.ToJdbcType(parameter.DbType), parameter.Scale);
            }
        }

        /// <summary>
        /// Executes the command against its connection object, returning the number of rows affected.
        /// </summary>
        /// <returns></returns>
        public override int ExecuteNonQuery()
        {
            using (var lck = new ExecutingLock(this))
            {
                if (_connection == null)
                    throw new JdbcException("Connection must be available.");

                if (_connection.State != ConnectionState.Open)
                    throw new JdbcException("Connection must be open.");

                if (_connection._connection is null)
                    throw new InvalidOperationException();

                try
                {
                    var type = CommandType;
                    var text = ExtractCommandExtension(CommandText, out var flags);

                    // always prepare if we're going to use parameters
                    var hasAutoGeneratedKeys = (flags & JdbcCommandExtensionFlag.GetGeneratedKeys) != 0;
                    if (hasAutoGeneratedKeys || _parameters.Count > 0)
                        Prepare(type, text, hasAutoGeneratedKeys);

                    // if we're doing an implicit return value for a stored procedure, it is always index 0, and everything is offset
                    var offset = type == CommandType.StoredProcedure ? 1 : 0;

                    if (_prepared is not null)
                    {
                        foreach (JdbcParameter parameter in _parameters)
                            ApplyParameterValue(_prepared, parameter, offset);

                        try
                        {
                            // mark statement as excuting
                            _executing = _prepared;

                            // release lock while executing, but reattain upon completion
                            lck.Exit();
                            try
                            {
                                _prepared.setQueryTimeout(CommandTimeout);
                                _prepared.execute();
                            }
                            finally
                            {
                                lck.Enter();
                            }

                            return _prepared.getUpdateCount();
                        }
                        finally
                        {
                            _executing = null;
                        }
                    }
                    else
                    {
                        try
                        {
                            _executing = _connection._connection.createStatement();
                            _executing.setQueryTimeout(CommandTimeout);

                            // release lock while executing, but reattain upon completion
                            lck.Exit();
                            try
                            {
                                switch (type)
                                {
                                    case CommandType.Text:
                                        _executing.execute(text);
                                        break;
                                    case CommandType.StoredProcedure:
                                        if (hasAutoGeneratedKeys)
                                            throw new JdbcException("GetGeneratedKeys not supported when executing Stored Procedures.");
                                        else
                                            _executing.execute(BuildJdbcStoredProcedureCallString(text));
                                        break;
                                    case CommandType.TableDirect:
                                        throw new NotImplementedException();
                                }
                            }
                            finally
                            {
                                lck.Enter();
                            }

                            return _executing.getUpdateCount();
                        }
                        finally
                        {
                            _executing = null;
                        }
                    }

                    throw new InvalidOperationException();
                }
                catch (SQLException e)
                {
                    throw new JdbcException(e);
                }
            }
        }

        /// <summary>
        /// Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.
        /// </summary>
        /// <returns></returns>
        public override object? ExecuteScalar()
        {
            // execute a reader with a single result
            using var rdr = ExecuteDbDataReader(CommandBehavior.SingleResult);
            while (rdr.Read())
                return rdr[0];

            return null;
        }

        /// <summary>
        /// Executes the command against its connection, returning a <see cref="DbDataReader"/> which can be used to access the results.
        /// </summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            using (var lck = new ExecutingLock(this))
            {
                if (_connection == null)
                    throw new JdbcException("Connection must be available.");

                if (_connection.State != ConnectionState.Open)
                    throw new JdbcException("Connection must be open.");

                if (_connection._connection is null)
                    throw new InvalidOperationException();

                try
                {
                    var type = CommandType;
                    var text = ExtractCommandExtension(CommandText, out var flags);

                    // always prepare if we're going to use parameters
                    var hasAutoGeneratedKeys = (flags & JdbcCommandExtensionFlag.GetGeneratedKeys) != 0;
                    if (hasAutoGeneratedKeys || _parameters.Count > 0)
                        Prepare(type, text, hasAutoGeneratedKeys);

                    // if we're doing an implicit return value for a stored procedure, it is always index 0, adn everything is offset
                    var offset = type == CommandType.StoredProcedure ? 1 : 0;

                    if (_prepared is not null)
                    {
                        foreach (JdbcParameter parameter in _parameters)
                            ApplyParameterValue(_prepared, parameter, offset);

                        try
                        {
                            // mark statement as executing
                            _executing = _prepared;

                            // release lock while executing, but reattain upon completion
                            lck.Exit();
                            try
                            {
                                _prepared.setQueryTimeout(CommandTimeout);
                                _prepared.execute();
                            }
                            finally
                            {
                                lck.Enter();
                            }

                            return new JdbcDataReader((JdbcCommand)this, _executing, hasAutoGeneratedKeys);
                        }
                        finally
                        {
                            _executing = null;
                        }
                    }
                    else
                    {
                        try
                        {
                            _executing = _connection._connection.createStatement();
                            _executing.setQueryTimeout(CommandTimeout);

                            // release lock while executing, but reattain upon completion
                            lck.Exit();
                            try
                            {
                                switch (type)
                                {
                                    case CommandType.Text:
                                        _executing.execute(CommandText);
                                        break;
                                    case CommandType.StoredProcedure:
                                        if (hasAutoGeneratedKeys)
                                            throw new JdbcException("GetGeneratedKeys not supported when executing Stored Procedures.");
                                        else
                                            _executing.execute(BuildJdbcStoredProcedureCallString(text));
                                        break;
                                    case CommandType.TableDirect:
                                        throw new NotImplementedException();
                                }
                            }
                            finally
                            {
                                lck.Enter();
                            }

                            return new JdbcDataReader((JdbcCommand)this, _executing, hasAutoGeneratedKeys);
                        }
                        finally
                        {
                            _executing = null;
                        }
                    }

                    throw new InvalidOperationException();
                }
                catch (SQLException e)
                {
                    throw new JdbcException(e);
                }
            }
        }

        /// <summary>
        /// Attempts to cancel the execution of a DbCommand.
        /// </summary>
        public override void Cancel()
        {
            lock (_syncRoot)
                if (_executing != null)
                    _executing.cancel();
        }

    }

}
