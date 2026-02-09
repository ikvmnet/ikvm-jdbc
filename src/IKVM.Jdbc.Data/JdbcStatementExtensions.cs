using System;
using System.Data;
using System.IO;
using System.Text;
using System.Xml;
using System.Xml.Linq;

using ikvm.io;

using java.io;
using java.math;
using java.sql;
using java.time;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Extension ethods for <see cref="Statement"/> instances.
    /// </summary>
    static class JdbcStatementExtensions
    {

        static readonly Version JDBC_4_2 = new Version(4, 2);

        /// <summary>
        /// Sets the value of the parameter at the specified index to the given type and value.
        /// </summary>
        /// <param name="statement"></param>
        /// <param name="index"></param>
        /// <param name="type"></param>
        /// <param name="value"></param>
        public static void SetParameterValue(this PreparedStatement statement, Version jdbcVersion, int index, DbType type, object? value)
        {
            if (statement is null)
                throw new ArgumentNullException(nameof(statement));
            if (jdbcVersion is null)
                throw new ArgumentNullException(nameof(jdbcVersion));

            if (value == null || value == DBNull.Value)
            {
                statement.setNull(index, JdbcDbType.ToJdbcType(type).ordinal());
                return;
            }

            switch (type)
            {
                case DbType.AnsiString:
                    switch (value)
                    {
                        case string s:
                            statement.setString(index, (string)value);
                            break;
                        case byte[] c:
                            statement.setAsciiStream(index, new ByteArrayInputStream(c), c.Length);
                            break;
                        case char[] c:
                            statement.setAsciiStream(index, new ByteArrayInputStream(Encoding.ASCII.GetBytes(c)), c.Length);
                            break;
                        case Stream s:
                            statement.setAsciiStream(index, new InputStreamWrapper(s), s.Length);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Binary:
                    switch (value)
                    {
                        case byte[] b:
                            statement.setBytes(index, b);
                            break;
                        case Stream b:
                            statement.setBinaryStream(index, new InputStreamWrapper(b), b.Length);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Byte:
                    switch (value)
                    {
                        case byte b:
                            statement.setByte(index, b);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Boolean:
                    switch (value)
                    {
                        case bool b:
                            statement.setBoolean(index, b);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Currency:
                    switch (value)
                    {
                        case decimal d:
                            statement.setBigDecimal(index, new BigDecimal(d.ToString()));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Date:
                    switch (value)
                    {
#if NET
                        case DateOnly d when jdbcVersion >= JDBC_4_2:
                            statement.setObject(index, LocalDate.of(d.Year, d.Month, d.Day));
                            break;
                        case DateOnly d:
                            statement.setDate(index, new Date(d.Year - 1900, d.Month - 1, d.Day));
                            break;
#endif
                        case DateTime dt when jdbcVersion >= JDBC_4_2:
                            statement.setObject(index, LocalDate.of(dt.Year, dt.Month, dt.Day));
                            break;
                        case DateTime dt:
                            statement.setDate(index, new Date(new DateTimeOffset(dt).ToUnixTimeMilliseconds()));
                            break;
                        case DateTimeOffset dt:
                            statement.setDate(index, new Date(dt.ToUnixTimeMilliseconds()));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.DateTime:
                    switch (value)
                    {
                        case DateTime dt:
                            statement.setTimestamp(index, new Timestamp(new DateTimeOffset(dt).ToUnixTimeMilliseconds()));
                            break;
                        case DateTimeOffset dt:
                            statement.setTimestamp(index, new Timestamp(dt.ToUnixTimeMilliseconds()));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Decimal:
                    switch (value)
                    {
                        case decimal d:
                            statement.setBigDecimal(index, new BigDecimal(d.ToString()));
                            break;
                        case int d:
                            statement.setBigDecimal(index, new BigDecimal(d));
                            break;
                        case short d:
                            statement.setBigDecimal(index, new BigDecimal(d));
                            break;
                        case long d:
                            statement.setBigDecimal(index, new BigDecimal(d));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Double:
                    switch (value)
                    {
                        case double d:
                            statement.setDouble(index, d);
                            break;
                        case float d:
                            statement.setDouble(index, d);
                            break;
                        case int d:
                            statement.setDouble(index, d);
                            break;
                        case short d:
                            statement.setDouble(index, d);
                            break;
                        case long d:
                            statement.setDouble(index, d);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Guid:
                    switch (value)
                    {
                        case Guid d:
                            statement.setString(index, d.ToString());
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Int16:
                    switch (value)
                    {
                        case short d:
                            statement.setShort(index, d);
                            break;
                        case int d:
                            statement.setShort(index, (short)d);
                            break;
                        case long d:
                            statement.setShort(index, (short)d);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Int32:
                    switch (value)
                    {
                        case short d:
                            statement.setInt(index, d);
                            break;
                        case int d:
                            statement.setInt(index, d);
                            break;
                        case long d:
                            statement.setInt(index, checked((int)d));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Int64:
                    switch (value)
                    {
                        case short d:
                            statement.setLong(index, d);
                            break;
                        case int d:
                            statement.setLong(index, d);
                            break;
                        case long d:
                            statement.setLong(index, d);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Object:
                    throw new NotImplementedException();
                case DbType.SByte:
                    switch (value)
                    {
                        case byte d:
                            statement.setByte(index, d);
                            break;
                        case sbyte d:
                            statement.setByte(index, (byte)d);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Single:
                    switch (value)
                    {
                        case float d:
                            statement.setFloat(index, d);
                            break;
                        case double d:
                            statement.setFloat(index, (float)d);
                            break;
                        case short d:
                            statement.setFloat(index, (float)d);
                            break;
                        case int d:
                            statement.setFloat(index, (float)d);
                            break;
                        case long d:
                            statement.setFloat(index, (float)d);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.String:
                    switch (value)
                    {
                        case string d:
                            statement.setNString(index, d);
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Time:
                    switch (value)
                    {
#if NET
                        case TimeOnly t:
                            statement.setTime(index, new Time(new DateTimeOffset(new DateTime(1970, 1, 1).Add(t.ToTimeSpan())).ToUnixTimeMilliseconds()));
                            break;
#endif
                        case TimeSpan t:
                            statement.setTime(index, new Time(new DateTimeOffset(new DateTime(1970, 1, 1).Add(t)).ToUnixTimeMilliseconds()));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.UInt16:
                    switch (value)
                    {
                        case byte byte_:
                            statement.setShort(index, byte_);
                            break;
                        case short short_:
                            statement.setShort(index, short_);
                            break;
                        case int int_ when int_ <= short.MaxValue && int_ >= short.MinValue:
                            statement.setShort(index, checked((short)int_));
                            break;
                        case long long_ when long_ <= short.MaxValue && long_ >= short.MinValue:
                            statement.setShort(index, checked((short)long_));
                            break;
                        case ushort ushort_ when ushort_ <= short.MaxValue:
                            statement.setShort(index, checked((short)ushort_));
                            break;
                        default:
                            throw new JdbcTypeException("Coercion of UInt16 to Java 'short' failed: out of range");
                    }
                    break;
                case DbType.UInt32:
                    switch (value)
                    {
                        case byte byte_:
                            statement.setInt(index, byte_);
                            break;
                        case short short_:
                            statement.setInt(index, short_);
                            break;
                        case int int_:
                            statement.setInt(index, int_);
                            break;
                        case long long_ when long_ <= int.MaxValue && long_ >= int.MinValue:
                            statement.setInt(index, checked((short)long_));
                            break;
                        case uint uint_ when uint_ <= int.MaxValue:
                            statement.setInt(index, checked((int)uint_));
                            break;
                        default:
                            throw new JdbcTypeException("Coercion of UInt32 to Java 'int' failed: out of range");
                    }
                    break;
                case DbType.UInt64:
                    switch (value)
                    {
                        case byte byte_:
                            statement.setLong(index, byte_);
                            break;
                        case short short_:
                            statement.setLong(index, short_);
                            break;
                        case int int_:
                            statement.setLong(index, int_);
                            break;
                        case long long_:
                            statement.setLong(index, long_);
                            break;
                        case ulong ulong_ when ulong_ <= long.MaxValue:
                            statement.setLong(index, checked((long)ulong_));
                            break;
                        default:
                            throw new JdbcException("Coercion of UInt64 to Int64 failed: out of range");
                    }
                    break;
                case DbType.VarNumeric:
                    throw new NotSupportedException("Unsupported DbType VarNumeric.");
                case DbType.AnsiStringFixedLength:
                    switch (value)
                    {
                        case string s:
                            statement.setString(index, s);
                            break;
                        case char[] s:
                            statement.setString(index, new string(s));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.StringFixedLength:
                    switch (value)
                    {
                        case string s:
                            statement.setNString(index, s);
                            break;
                        case char[] s:
                            statement.setNString(index, new string(s));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.Xml:
                    switch (value)
                    {
                        case XNode dt:
                            {
                                var xml = statement.getConnection().createSQLXML();
                                xml.setString(dt.ToString());
                                statement.setSQLXML(index, xml);
                            }
                            break;
                        case XmlDocument doc:
                            {
                                var xml = statement.getConnection().createSQLXML();
                                var tmp = new XDocument();
                                doc.WriteTo(tmp.CreateWriter());
                                xml.setString(tmp.ToString());
                                statement.setSQLXML(index, xml);
                            }
                            break;
                        case XmlReader rdr:
                            {
                                var xml = statement.getConnection().createSQLXML();
                                xml.setString(XNode.ReadFrom(rdr).ToString());
                                statement.setSQLXML(index, xml);
                            }
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.DateTime2:
                    switch (value)
                    {
                        case DateTime dt:
                            statement.setTimestamp(index, new Timestamp(new DateTimeOffset(dt).ToUnixTimeMilliseconds()));
                            break;
                        case DateTimeOffset dt:
                            statement.setTimestamp(index, new Timestamp(dt.ToUnixTimeMilliseconds()));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.DateTimeOffset:
                    switch (value)
                    {
                        case DateTime dt:
                            statement.setTimestamp(index, new Timestamp(new DateTimeOffset(dt).ToUnixTimeMilliseconds()));
                            break;
                        case DateTimeOffset dt:
                            statement.setTimestamp(index, new Timestamp(dt.ToUnixTimeMilliseconds()));
                            break;
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
            }
        }

    }

}
