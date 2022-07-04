using System.Data;
using System.Data.Common;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Represents a parameter to a System.Data.Common.DbCommand and optionally, its mapping to a System.Data.DataSet
    /// column. For more information on parameters, see Configuring Parameters and Parameter Data Types (ADO.NET).
    /// </summary>
    public class JdbcParameter : DbParameter
    {

        /// <summary>
        /// Gets or sets the name of the <see cref="JdbcParameter"/>.
        /// </summary>
        public override string ParameterName { get; set; }

        /// <summary>
        /// Gets or sets the direction of the <see cref="JdbcParameter"/>.
        /// </summary>
        public override ParameterDirection Direction { get; set; }

        /// <summary>
        /// Gets or sets the type of the <see cref="JdbcParameter"/>.
        /// </summary>
        public override DbType DbType { get; set; }

        /// <summary>
        /// Resets the <see cref="JdbcParameter"/> type.
        /// </summary>
        public override void ResetDbType()
        {
            DbType = DbType.String;
        }

        /// <summary>
        /// Gets or sets whether the <see cref="JdbcParameter"/> represents a nullable type.
        /// </summary>
        public override bool IsNullable { get; set; }

        /// <summary>
        /// Gets or sets the size of the <see cref="JdbcParameter"/> type.
        /// </summary>
        public override int Size { get; set; }

        public override string SourceColumn { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        /// <summary>
        /// Gets or sets the value of the <see cref="JdbcParameter"/>.
        /// </summary>
        public override object Value { get; set; }

    }

}
