using System.Data.Common;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Strongly-typed JDBC connection string builder.
    /// </summary>
    public class JdbcConnectionStringBuilder : DbConnectionStringBuilder
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public JdbcConnectionStringBuilder()
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public JdbcConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Gets or sets the user to use when opening the connection.
        /// </summary>
        public string User
        {
            get => this["user"] as string;
            set => this["user"] = value;
        }

        /// <summary>
        /// Gets or sets the user to use when opening the connection.
        /// </summary>
        public string Password
        {
            get => this["password"] as string;
            set => this["password"] = value;
        }

        /// <summary>
        /// Gets or sets the JDBC URL.
        /// </summary>
        public string Url
        {
            get => this["url"] as string;
            set => this["url"] = value;
        }

    }

}
