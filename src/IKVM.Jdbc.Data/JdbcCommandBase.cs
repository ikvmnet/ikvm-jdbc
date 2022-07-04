using System;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Represents a SQL statement or stored procedure to execute against a <see cref="JdbcConnection"/>.
    /// </summary>
    public abstract class JdbcCommandBase : DbCommand
    {

        readonly object syncRoot = new object();
        readonly JdbcParameterCollection parameters = new JdbcParameterCollection();
        JdbcConnection connection;
        PreparedStatement prepared;
        Statement executing;

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
            this.connection = connection;
        }

        /// <summary>
        /// Executes the action if not already executing.
        /// </summary>
        /// <param name="action"></param>
        /// <exception cref="JdbcException"></exception>
        void ExecutingLock(Action action)
        {
            lock (syncRoot)
            {
                if (executing != null)
                    throw new JdbcException("The command is already executing a statement.");

                action();
            }
        }

        /// <summary>
        /// Executes the action if not already executing.
        /// </summary>
        /// <param name="func"></param>
        /// <exception cref="JdbcException"></exception>
        T ExecutingLock<T>(Func<T> func)
        {
            lock (syncRoot)
            {
                if (executing != null)
                    throw new JdbcException("The command is already executing a statement.");

                return func();
            }
        }

        /// <summary>
        /// Gets or sets the database connection.
        /// </summary>
        protected override DbConnection DbConnection
        {
            get => connection;
            set => ExecutingLock(() => connection = (JdbcConnection)value);
        }

        /// <summary>
        /// Gets or sets how the <see cref="CommandText"/> property is interpreted.
        /// </summary>
        public override CommandType CommandType { get; set; } = CommandType.Text;

        /// <summary>
        /// Gets or sets the text command to run against the data source.
        /// </summary>
        public override string CommandText { get; set; }

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
        protected override DbParameterCollection DbParameterCollection => parameters;

        /// <summary>
        /// Gets or sets the <see cref="DbTransaction"/> within which this <see cref="JdbcCommandBase"/> object executes.
        /// </summary>
        protected override DbTransaction DbTransaction { get; set; }

        /// <summary>
        /// Creates a new instance of a <see cref="DbParameter"/> object.
        /// </summary>
        /// <returns></returns>
        protected override DbParameter CreateDbParameter()
        {
            if (prepared != null)
                throw new JdbcException("Command is already prepared.");

            return new JdbcParameter();
        }

        /// <summary>
        /// Builds the JDBC escape string for a stored procedure call.
        /// </summary>
        /// <returns></returns>
        string BuildJdbcStoredProcedureCallString()
        {
            var b = new StringBuilder("{{ ");

            if (parameters.Any(i => i.Direction == ParameterDirection.ReturnValue))
                b.Append("? = ");

            for (int i = 0; i < parameters.Count; i++)
            {
                b.Append("?");
                if (i < parameters.Count - 1)
                    b.Append(", ");
            }

            b.Append(" }}");

            return b.ToString();
        }

        /// <summary>
        /// Creates a prepared (or compiled) version of the command on the data source.
        /// </summary>
        public override void Prepare()
        {
            ExecutingLock(() =>
            {
                if (connection == null)
                    throw new JdbcException("Connection must be available.");

                if (connection.State != ConnectionState.Open)
                    throw new JdbcException("Connection must be open.");

                if (prepared != null)
                    throw new JdbcException("Command is already prepared.");

                try
                {
                    switch (CommandType)
                    {
                        case CommandType.Text:
                            prepared = connection.connection.prepareStatement(CommandText);
                            break;
                        case CommandType.StoredProcedure:
                            prepared = connection.connection.prepareCall(BuildJdbcStoredProcedureCallString());
                            break;
                        case CommandType.TableDirect:
                            throw new NotImplementedException();
                    }
                }
                catch (SQLException e)
                {
                    throw new JdbcException(e);
                }
            });
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
            return ExecutingLock(() =>
            {
                if (connection == null)
                    throw new JdbcException("Connection must be available.");

                if (connection.State != ConnectionState.Open)
                    throw new JdbcException("Connection must be open.");

                try
                {
                    // always prepare if we're going to use parameters
                    if (parameters.Count > 0)
                        Prepare();

                    // if we're doing an implicit return value for a stored procedure, it is always index 0, adn everything is offset
                    var offset = CommandType == CommandType.StoredProcedure ? 1 : 0;

                    if (prepared is not null)
                    {
                        foreach (JdbcParameter parameter in parameters)
                            ApplyParameterValue(prepared, parameter, offset);

                        try
                        {
                            // mark statement as excuting
                            executing = prepared;

                            // release lock while executing, but reattain upon completion
                            Monitor.Exit(syncRoot);
                            try
                            {
                                prepared.setQueryTimeout(CommandTimeout);
                                prepared.execute();
                            }
                            finally
                            {
                                Monitor.Enter(syncRoot);
                            }

                            return prepared.getUpdateCount();
                        }
                        finally
                        {
                            executing = null;
                        }
                    }
                    else
                    {
                        try
                        {
                            executing = connection.connection.createStatement();
                            executing.setQueryTimeout(CommandTimeout);

                            // release lock while executing, but reattain upon completion
                            Monitor.Exit(syncRoot);
                            try
                            {
                                switch (CommandType)
                                {
                                    case CommandType.Text:
                                        executing.execute(CommandText);
                                        break;
                                    case CommandType.StoredProcedure:
                                        executing.execute(BuildJdbcStoredProcedureCallString());
                                        break;
                                    case CommandType.TableDirect:
                                        throw new NotImplementedException();
                                }
                            }
                            finally
                            {
                                Monitor.Enter(syncRoot);
                            }

                            return executing.getUpdateCount();
                        }
                        finally
                        {
                            executing = null;
                        }
                    }

                    throw new InvalidOperationException();
                }
                catch (SQLException e)
                {
                    throw new JdbcException(e);
                }
            });
        }

        /// <summary>
        /// Executes the command and returns the first column of the first row in the first returned result set. All other columns, rows and result sets are ignored.
        /// </summary>
        /// <returns></returns>
        public override object ExecuteScalar()
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
            return ExecutingLock(() =>
            {
                if (connection == null)
                    throw new JdbcException("Connection must be available.");

                if (connection.State != ConnectionState.Open)
                    throw new JdbcException("Connection must be open.");

                try
                {
                    // always prepare if we're going to use parameters
                    if (parameters.Count > 0)
                        Prepare();

                    // if we're doing an implicit return value for a stored procedure, it is always index 0, adn everything is offset
                    var offset = CommandType == CommandType.StoredProcedure ? 1 : 0;

                    if (prepared is not null)
                    {
                        foreach (JdbcParameter parameter in parameters)
                            ApplyParameterValue(prepared, parameter, offset);

                        try
                        {
                            // mark statement as excuting
                            executing = prepared;

                            ResultSet rs;

                            // release lock while executing, but reattain upon completion
                            Monitor.Exit(syncRoot);
                            try
                            {
                                prepared.setQueryTimeout(CommandTimeout);
                                rs = prepared.executeQuery();
                            }
                            finally
                            {
                                Monitor.Enter(syncRoot);
                            }

                            return new JdbcDataReader(rs, prepared.getUpdateCount());
                        }
                        finally
                        {
                            executing = null;
                        }
                    }
                    else
                    {
                        try
                        {
                            executing = connection.connection.createStatement();
                            executing.setQueryTimeout(CommandTimeout);

                            ResultSet rs = null;

                            // release lock while executing, but reattain upon completion
                            Monitor.Exit(syncRoot);
                            try
                            {
                                switch (CommandType)
                                {
                                    case CommandType.Text:
                                        rs = executing.executeQuery(CommandText);
                                        break;
                                    case CommandType.StoredProcedure:
                                        rs = executing.executeQuery(BuildJdbcStoredProcedureCallString());
                                        break;
                                    case CommandType.TableDirect:
                                        throw new NotImplementedException();
                                }
                            }
                            finally
                            {
                                Monitor.Enter(syncRoot);
                            }

                            return new JdbcDataReader(rs, executing.getUpdateCount());
                        }
                        finally
                        {
                            executing = null;
                        }
                    }

                    throw new InvalidOperationException();
                }
                catch (SQLException e)
                {
                    throw new JdbcException(e);
                }
            });
        }

        /// <summary>
        /// Attempts to cancel the execution of a DbCommand.
        /// </summary>
        public override void Cancel()
        {
            lock (syncRoot)
                if (executing != null)
                    executing.cancel();
        }

    }

}
