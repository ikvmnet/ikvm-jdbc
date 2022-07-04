using System;
using System.Collections;
using System.Data.Common;
using System.Xml.Linq;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    public abstract class JdbcDataReaderBase : DbDataReader
    {

        ResultSet rs;
        bool hasRows;
        int recordsAffected;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rs"></param>
        /// <param name="recordsAffected"></param>
        internal JdbcDataReaderBase(ResultSet rs, int recordsAffected)
        {
            this.rs = rs ?? throw new ArgumentNullException(nameof(rs));
            this.recordsAffected = recordsAffected;
            this.hasRows = rs.isBeforeFirst();
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override object this[int ordinal] => GetValue(ordinal);

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public override object this[string name] => GetValue(GetOrdinal(name));

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        public override int Depth => 0;

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public override int FieldCount => rs.getMetaData().getColumnCount();

        /// <summary>
        /// Gets a value that indiciates whether this <see cref="JdbcDataReaderBase"/> has one or more rows.
        /// </summary>
        public override bool HasRows => hasRows;

        /// <summary>
        /// Gets a value indicating whether this <see cref="JdbcDataReaderBase"/> is closed.
        /// </summary>
        public override bool IsClosed => rs.isClosed();

        /// <summary>
        /// Gets the number of rows changed, inserted or deleted by the SQL statement.
        /// </summary>
        public override int RecordsAffected => recordsAffected;

        /// <summary>
        /// Gets an <see cref="IEnumerator"/> that can be used to iterate through the rows in the <see cref="JdbcDataReaderBase"/>.
        /// </summary>
        /// <returns></returns>
        public override IEnumerator GetEnumerator()
        {
            return new JdbcEnumerator(this);
        }

        /// <summary>
        /// Gets the data type of the specified column.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <exception cref="NotImplementedException"></exception>
        public override Type GetFieldType(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            var type = rs.getMetaData().getColumnType(ordinal + 1);

            if (type == JDBCType.ARRAY.ordinal())
                return typeof(System.Array);

            if (type == JDBCType.BIGINT.ordinal())
                return typeof(long);

            if (type == JDBCType.BINARY.ordinal())
                return typeof(byte[]);

            if (type == JDBCType.BIT.ordinal())
                return typeof(bool);

            if (type == JDBCType.BLOB.ordinal())
                return typeof(byte[]);

            if (type == JDBCType.BOOLEAN.ordinal())
                return typeof(bool);

            if (type == JDBCType.CHAR.ordinal())
                return typeof(string);

            if (type == JDBCType.CLOB.ordinal())
                return typeof(string);

            if (type == JDBCType.DATALINK.ordinal())
                throw new NotSupportedException();

            if (type == JDBCType.DATE.ordinal())
                return typeof(DateTime);

            if (type == JDBCType.DECIMAL.ordinal())
                return typeof(decimal);

            if (type == JDBCType.DISTINCT.ordinal())
                throw new NotImplementedException();

            if (type == JDBCType.DOUBLE.ordinal())
                return typeof(double);

            if (type == JDBCType.FLOAT.ordinal())
                return typeof(float);

            if (type == JDBCType.INTEGER.ordinal())
                return typeof(int);

            if (type == JDBCType.JAVA_OBJECT.ordinal())
                return typeof(object);

            if (type == JDBCType.LONGNVARCHAR.ordinal())
                return typeof(string);

            if (type == JDBCType.LONGVARBINARY.ordinal())
                return typeof(byte[]);

            if (type == JDBCType.LONGVARCHAR.ordinal())
                return typeof(string);

            if (type == JDBCType.NCHAR.ordinal())
                return typeof(string);

            if (type == JDBCType.NCLOB.ordinal())
                return typeof(string);

            if (type == JDBCType.NULL.ordinal())
                return typeof(object);

            if (type == JDBCType.NUMERIC.ordinal())
                throw new NotImplementedException();

            if (type == JDBCType.NVARCHAR.ordinal())
                return typeof(string);

            if (type == JDBCType.OTHER.ordinal())
                throw new NotSupportedException();

            if (type == JDBCType.REAL.ordinal())
                return typeof(float);

            if (type == JDBCType.REF.ordinal())
                throw new NotSupportedException();

            if (type == JDBCType.REF_CURSOR.ordinal())
                throw new NotSupportedException();

            if (type == JDBCType.ROWID.ordinal())
                throw new NotImplementedException();

            if (type == JDBCType.SMALLINT.ordinal())
                return typeof(short);

            if (type == JDBCType.SQLXML.ordinal())
                return typeof(XDocument);

            if (type == JDBCType.STRUCT.ordinal())
                throw new NotSupportedException();

            if (type == JDBCType.TIME.ordinal())
                return typeof(TimeSpan);

            if (type == JDBCType.TIMESTAMP.ordinal())
                return typeof(DateTime);

            if (type == JDBCType.TIMESTAMP_WITH_TIMEZONE.ordinal())
                return typeof(DateTimeOffset);

            if (type == JDBCType.TIME_WITH_TIMEZONE.ordinal())
                return typeof(DateTimeOffset);

            if (type == JDBCType.TINYINT.ordinal())
                return typeof(byte);

            if (type == JDBCType.VARBINARY.ordinal())
                return typeof(byte[]);

            if (type == JDBCType.VARCHAR.ordinal())
                return typeof(string);

            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets the name of the data type of the specified column.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override string GetDataTypeName(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            return rs.getMetaData().getColumnTypeName(ordinal);
        }

        /// <summary>
        /// Gets the name of the column, given the zero based column ordinal.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override string GetName(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            return rs.getMetaData().getColumnName(ordinal + 1);
        }

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public override int GetOrdinal(string name)
        {
            if (name is null)
                throw new ArgumentNullException(nameof(name));

            return rs.findColumn(name) is int i && i > 0 ? i - 1 : -1;
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public override object GetValue(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            // java starts at 1
            ordinal++;
            var type = rs.getMetaData().getColumnType(ordinal);

            if (type == JDBCType.ARRAY.ordinal())
            {
                var v = rs.getArray(ordinal);
                return rs.wasNull() ? DBNull.Value : v.getArray();
            }

            if (type == JDBCType.BIGINT.ordinal())
            {
                var v = rs.getLong(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.BINARY.ordinal())
            {
                var v = rs.getBytes(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.BIT.ordinal())
            {
                var v = rs.getBoolean(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.BLOB.ordinal())
            {
                var v = rs.getBlob(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.BOOLEAN.ordinal())
            {
                var v = rs.getBoolean(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.CHAR.ordinal())
            {
                var v = rs.getString(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.CLOB.ordinal())
            {
                var v = rs.getClob(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.DATALINK.ordinal())
            {
                throw new NotSupportedException();
            }

            if (type == JDBCType.DATE.ordinal())
            {
                var v = rs.getDate(ordinal);
                return rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(v.getTime()).UtcDateTime;
            }

            if (type == JDBCType.DECIMAL.ordinal())
            {
                var v = rs.getBigDecimal(ordinal);
                return rs.wasNull() ? DBNull.Value : decimal.Parse(v.toString());
            }

            if (type == JDBCType.DISTINCT.ordinal())
            {
                throw new NotImplementedException();
            }

            if (type == JDBCType.DOUBLE.ordinal())
            {
                var v = rs.getDouble(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.FLOAT.ordinal())
            {
                var v = rs.getFloat(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.INTEGER.ordinal())
            {
                var v = rs.getInt(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.JAVA_OBJECT.ordinal())
            {
                var v = rs.getObject(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.LONGNVARCHAR.ordinal())
            {
                var v = rs.getNString(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.LONGVARBINARY.ordinal())
            {
                var v = rs.getBytes(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.LONGVARCHAR.ordinal())
            {
                var v = rs.getString(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.NCHAR.ordinal())
            {
                var v = rs.getNString(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.NCLOB.ordinal())
            {
                var v = rs.getNClob(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.NULL.ordinal())
            {
                return DBNull.Value;
            }

            if (type == JDBCType.NUMERIC.ordinal())
            {
                throw new NotImplementedException();
                //var v = rs.getBigDecimal(ordinal);
                //return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.NVARCHAR.ordinal())
            {
                var v = rs.getNString(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.OTHER.ordinal())
            {
                throw new NotSupportedException();
            }

            if (type == JDBCType.REAL.ordinal())
            {
                var v = rs.getFloat(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.REF.ordinal())
            {
                throw new NotSupportedException();
            }

            if (type == JDBCType.REF_CURSOR.ordinal())
            {
                throw new NotSupportedException();
            }

            if (type == JDBCType.ROWID.ordinal())
            {
                throw new NotImplementedException();
            }

            if (type == JDBCType.SMALLINT.ordinal())
            {
                var v = rs.getShort(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.SQLXML.ordinal())
            {
                var v = rs.getSQLXML(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.STRUCT.ordinal())
            {
                throw new NotSupportedException();
            }

            if (type == JDBCType.TIME.ordinal())
            {
                var v = rs.getTime(ordinal);
                return rs.wasNull() ? DBNull.Value : TimeSpan.FromMilliseconds(v.getTime());
            }

            if (type == JDBCType.TIMESTAMP.ordinal())
            {
                var v = rs.getTime(ordinal);
                return rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(v.getTime()).UtcDateTime;
            }

            if (type == JDBCType.TIMESTAMP_WITH_TIMEZONE.ordinal())
            {
                var v = rs.getTime(ordinal);
                return rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(v.getTime());
            }

            if (type == JDBCType.TIME_WITH_TIMEZONE.ordinal())
            {
                var v = rs.getTime(ordinal);
                return rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(v.getTime());
            }

            if (type == JDBCType.TINYINT.ordinal())
            {
                var v = rs.getByte(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.VARBINARY.ordinal())
            {
                var v = rs.getBytes(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            if (type == JDBCType.VARCHAR.ordinal())
            {
                var v = rs.getString(ordinal);
                return rs.wasNull() ? DBNull.Value : v;
            }

            throw new NotSupportedException();
        }

        /// <summary>
        /// Populates an array of <see cref="object"/> with the column values of the current row.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public override int GetValues(object[] values)
        {
            if (values is null)
                throw new ArgumentNullException(nameof(values));

            var n = rs.getMetaData().getColumnCount();
            for (int i = 0; i < n; i++)
                values[i] = GetValue(i);

            return n;
        }

        /// <summary>
        /// Gets a value that indicates whether the column contains non-existent or null values.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override bool IsDBNull(int ordinal)
        {
            GetValue(ordinal);
            return rs.wasNull();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="bool"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override bool GetBoolean(int ordinal)
        {
            return (bool)GetValue(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="byte"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override byte GetByte(int ordinal)
        {
            return (byte)GetValue(ordinal);
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column, starting at location indicated by <paramref name="dataOffset"/>, into the
        /// buffer, starting at the location indicated by <paramref name="bufferOffset"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferOffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        /// <exception cref="JdbcException"></exception>
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            return GetBytes(ordinal, dataOffset, buffer.AsSpan().Slice(bufferOffset, length));
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column, starting at location indicated by <paramref name="dataOffset"/>, into the
        /// <see cref="Span{Byte}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public long GetBytes(int ordinal, long dataOffset, Span<byte> buffer)
        {
            var v = GetValue(ordinal);
            switch (v)
            {
                case null:
                case DBNull:
                    throw new JdbcException("Could not write null value to string.");
                case byte[] c:
                    var _2 = c.AsSpan().Slice((int)dataOffset);
                    _2.CopyTo(buffer);
                    return Math.Min(_2.Length, buffer.Length);
                case Blob n:
                    throw new NotImplementedException($"Could not convert blob into bytes.");
                default:
                    throw new JdbcException($"Could not convert {v.GetType()} into string.");
            }
        }

        /// <summary>
        /// Gets the value of the specified column as a single <see cref="char"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override char GetChar(int ordinal)
        {
            return (char)GetValue(ordinal);
        }

        /// <summary>
        /// Reads a stream of characters from the specified column, starting at location indicated by <paramref name="dataOffset"/>, into the
        /// buffer, starting at the location indicated by <paramref name="bufferOffset"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferOffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        /// <exception cref="JdbcException"></exception>
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            return GetChars(ordinal, dataOffset, buffer.AsSpan(bufferOffset, length));
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column, starting at location indicated by <paramref name="dataOffset"/>, into the
        /// <see cref="Span{Char}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public long GetChars(int ordinal, long dataOffset, Span<char> buffer)
        {
            var v = GetValue(ordinal);
            switch (v)
            {
                case null:
                case DBNull:
                    throw new JdbcException("Could not write null value to string.");
                case string s:
                    var _1 = s.AsSpan().Slice((int)dataOffset);
                    _1.CopyTo(buffer);
                    return Math.Min(_1.Length, buffer.Length);
                case char[] c:
                    var _2 = c.AsSpan().Slice((int)dataOffset);
                    _2.CopyTo(buffer);
                    return Math.Min(_2.Length, buffer.Length);
                case NClob n:
                case Clob c:
                    throw new NotImplementedException($"Could not convert (n)clob into string.");
                default:
                    throw new JdbcException($"Could not convert {v.GetType()} into string.");
            }
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateTime"/> object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)GetValue(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="decimal"/> object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)GetValue(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="double"/> object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override double GetDouble(int ordinal)
        {
            return (double)GetValue(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="float"/> object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override float GetFloat(int ordinal)
        {
            return (float)GetValue(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="Guid"/> object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override Guid GetGuid(int ordinal)
        {
            return Guid.Parse((string)GetValue(ordinal));
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="short"/> object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override short GetInt16(int ordinal)
        {
            return (short)GetValue(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="int"/> object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override int GetInt32(int ordinal)
        {
            return (int)GetValue(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="long"/> object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override long GetInt64(int ordinal)
        {
            return (long)GetValue(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="string"/> object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override string GetString(int ordinal)
        {
            var v = GetValue(ordinal);
            return v switch
            {
                string s => s,
                NClob nclob => throw new NotImplementedException(),
                Clob clob => throw new NotImplementedException(),
                _ => throw new JdbcException($"Could not convert {v.GetType()} into string."),
            };
        }

        /// <summary>
        /// Advances the reader to the next record in the result set.
        /// </summary>
        /// <returns></returns>
        public override bool Read()
        {
            if (rs == null)
                throw new JdbcException("JdbcReader is closed.");

            return rs.next();
        }

        /// <summary>
        /// Advances the reader to the next result when reading the results of a batch of statements.
        /// </summary>
        /// <returns></returns>
        public override bool NextResult()
        {
            rs = null;
            hasRows = false;
            recordsAffected = 0;
            return false;
        }

        /// <summary>
        /// Closes the <see cref="JdbcDataReader"/> object.
        /// </summary>
        public override void Close()
        {
            if (rs != null)
            {
                rs.close();
                rs = null;
                hasRows = false;
                recordsAffected = 0;
            }

            base.Close();
        }

    }

}
