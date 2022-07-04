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
        /// Gets the collection of <see cref="JdbcParameter"/> objects.
        /// </summary>
        public new JdbcParameterCollection Parameters
        {
            get => JdbcParameters;
        }

        /// <summary>
        /// Creates a new instance of a <see cref="JdbcParameter"/> object.
        /// </summary>
        /// <returns></returns>
        public new JdbcParameter CreateDbParameter()
        {
            return CreateJdbcParameter();
        }

        /// <summary>
        /// Executes the command against its connection, returning a <see cref="JdbcDataReader"/> which can be used to access the results.
        /// </summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        public new JdbcDataReader ExecuteDataReader(CommandBehavior behavior)
        {
            return ExecuteJdbcDataReader(behavior);
        }

    }

}
