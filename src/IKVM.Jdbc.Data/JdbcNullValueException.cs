namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// The exception that is thrown when the value of a data field is null.
    /// </summary>
    public class JdbcNullValueException : JdbcException
    {

        /// <summary>
        /// Initiatlizes a new instance.
        /// </summary>
        public JdbcNullValueException() :
            base("Data is Null. This method or property cannot be called on Null values.")
        {

        }

    }

}
