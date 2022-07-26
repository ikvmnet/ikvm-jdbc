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

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Extension ethods for <see cref="Statement"/> instances.
    /// </summary>
    public static class JdbcStatementExtensions
    {

        /// <summary>
        /// Sets the value of the parameter at the specified index to the given type and value.
        /// </summary>
        /// <param name="statement"></param>
        /// <param name="index"></param>
        /// <param name="type"></param>
        /// <param name="value"></param>
        public static void SetParameterValue(this PreparedStatement statement, int index, DbType type, object value)
        {
            if (statement is null)
                throw new ArgumentNullException(nameof(statement));

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
                            statement.setInt(index, (int)d);
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
                        case TimeSpan t:
                            statement.setTime(index, new Time(new DateTimeOffset(new DateTime(1970, 1, 1).Add(t)).ToUnixTimeMilliseconds()));
                            break;
#if NET6_0_OR_GREATER
                        case TimeOnly t:
                            statement.setTime(index, new Time(new DateTimeOffset(new DateTime(1970, 1, 1).Add(t.ToTimeSpan())).ToUnixTimeMilliseconds()));
                            break;
#endif
                        default:
                            throw new JdbcException("Invalid parameter.");
                    }
                    break;
                case DbType.UInt16:
                    throw new NotSupportedException("Unsupported DbType UInt16.");
                case DbType.UInt32:
                    throw new NotSupportedException("Unsupported DbType UInt32.");
                case DbType.UInt64:
                    throw new NotSupportedException("Unsupported DbType UInt64.");
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
