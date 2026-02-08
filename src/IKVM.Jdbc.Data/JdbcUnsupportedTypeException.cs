using System;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// The exception that is thrown when the value of a data field is null.
    /// </summary>
    public class JdbcUnsupportedTypeException : JdbcException
    {

        /// <summary>
        /// Initiatlizes a new instance.
        /// </summary>
        public JdbcUnsupportedTypeException(string message, Version minVersion) :
            base(message)
        {

        }

    }

}
