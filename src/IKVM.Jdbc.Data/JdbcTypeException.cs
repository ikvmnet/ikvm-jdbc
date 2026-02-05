namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// The exception that is thrown when a type conversion or coercion error occurs.
    /// </summary>
    public class JdbcTypeException : JdbcException
    {

        /// <summary>
        /// Initiatlizes a new instance.
        /// </summary>
        public JdbcTypeException(string message) :
            base(message)
        {

        }

    }

}
