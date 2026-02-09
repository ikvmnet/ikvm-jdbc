using System;
using System.Data;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Static methods for working with <see cref="DbType"/>.
    /// </summary>
    static class JdbcDbType
    {

        /// <summary>
        /// Gets the Java <see cref="SQLType"/> that corresponds to the given .NET <see cref="DbType"/>.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static int ToSqlType(DbType type) => type switch
        {
            DbType.AnsiString => Types.VARCHAR,
            DbType.Binary => Types.BINARY,
            DbType.Byte => Types.TINYINT,
            DbType.Boolean => Types.BOOLEAN,
            DbType.Currency => Types.DECIMAL,
            DbType.Date => Types.DATE,
            DbType.DateTime => Types.TIMESTAMP,
            DbType.Decimal => Types.DECIMAL,
            DbType.Double => Types.DOUBLE,
            DbType.Guid => Types.VARCHAR,
            DbType.Int16 => Types.SMALLINT,
            DbType.Int32 => Types.INTEGER,
            DbType.Int64 => Types.BIGINT,
            DbType.Object => Types.JAVA_OBJECT,
            DbType.SByte => Types.TINYINT,
            DbType.Single => Types.FLOAT,
            DbType.String => Types.NVARCHAR,
            DbType.Time => Types.TIME,
            DbType.UInt16 => Types.SMALLINT,
            DbType.UInt32 => Types.INTEGER,
            DbType.UInt64 => Types.BIGINT,
            DbType.VarNumeric => Types.NUMERIC,
            DbType.AnsiStringFixedLength => Types.CHAR,
            DbType.StringFixedLength => Types.NCHAR,
            DbType.Xml => Types.SQLXML,
            DbType.DateTime2 => Types.TIMESTAMP,
            DbType.DateTimeOffset => Types.TIMESTAMP_WITH_TIMEZONE,
            _ => throw new JdbcTypeException("Type not supported."),
        };

    }

}