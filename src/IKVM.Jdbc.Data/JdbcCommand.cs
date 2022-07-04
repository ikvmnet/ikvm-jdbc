using System.Data;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Represents a SQL statement or stored procedure to execute against a <see cref="JdbcConnection"/>.
    /// </summary>
    public sealed class JdbcCommand : JdbcCommandBase
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public JdbcCommand()
        {

        }

        /// <summary>
        /// Intializes a new instance.
        /// </summary>
        /// <param name="connection"></param>
        public JdbcCommand(JdbcConnection connection) :
            base(connection)
        {

        }

        /// <summary>
        /// Gets the connection associated with this command.
        /// </summary>
        public new JdbcConnection Connection
        {
            get => (JdbcConnection)base.Connection;
            set => base.Connection = value;
        }

        /// <summary>
        /// Gets the collection of <see cref="JdbcParameter"/> objects.
        /// </summary>
        public new JdbcParameterCollection Parameters
        {
            get => (JdbcParameterCollection)base.Parameters;
        }

        /// <summary>
        /// Creates a new instance of a <see cref="JdbcParameter"/> object.
        /// </summary>
        /// <returns></returns>
        public new JdbcParameter CreateParameter()
        {
            return (JdbcParameter)base.CreateParameter();
        }

        /// <summary>
        /// Executes the command against its connection, returning a <see cref="JdbcDataReader"/> which can be used to access the results.
        /// </summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        public new JdbcDataReader ExecuteReader(CommandBehavior behavior)
        {
            return (JdbcDataReader)base.ExecuteReader(behavior);
        }

        /// <summary>
        /// Executes the command against its connection, returning a <see cref="JdbcDataReader"/> which can be used to access the results.
        /// </summary>
        /// <returns></returns>
        public new JdbcDataReader ExecuteReader()
        {
            return (JdbcDataReader)base.ExecuteReader();
        }

    }

}
