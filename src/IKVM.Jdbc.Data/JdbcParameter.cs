using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace IKVM.Jdbc.Data
{

    /// <inheritdoc />
    public class JdbcParameter : DbParameter
    {

        /// <inheritdoc />
#if NET
        [AllowNull]
#endif
        public override string ParameterName { get; set; } = "";

        /// <inheritdoc />
        public override ParameterDirection Direction { get; set; }

        /// <inheritdoc />
        public override DbType DbType { get; set; }

        /// <inheritdoc />
        public override void ResetDbType()
        {
            DbType = DbType.String;
        }

        /// <inheritdoc />
        public override bool IsNullable { get; set; }

        /// <inheritdoc />
        public override int Size { get; set; }

        /// <inheritdoc />
#if NET
        [AllowNull]
#endif
        public override string SourceColumn { get; set; } = "";

        /// <inheritdoc />
        public override bool SourceColumnNullMapping { get; set; }

        /// <inheritdoc />
        public override object? Value { get; set; }

    }

}
