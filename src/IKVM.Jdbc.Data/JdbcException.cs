using System;
using System.Data.Common;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Represents an exception that occurred accessing a <see cref="JdbcConnection"/>.
    /// </summary>
    public class JdbcException : DbException
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public JdbcException()
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="message"></param>
        public JdbcException(string message) :
            base(message)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public JdbcException(string message, Exception innerException) :
            base(message, innerException)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="jdbcException"></param>
        public JdbcException(java.sql.SQLException jdbcException) :
            this(jdbcException.getMessage(), jdbcException)
        {

        }

    }

}
