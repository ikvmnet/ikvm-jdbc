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
            get => this["User"] as string;
            set => this["User"] = value;
        }

        /// <summary>
        /// Gets or sets the user to use when opening the connection.
        /// </summary>
        public string Password
        {
            get => this["Password"] as string;
            set => this["Password"] = value;
        }

        /// <summary>
        /// Gets or sets the JDBC URL.
        /// </summary>
        public string Url
        {
            get => this["Url"] as string;
            set => this["Url"] = value;
        }

    }

}
