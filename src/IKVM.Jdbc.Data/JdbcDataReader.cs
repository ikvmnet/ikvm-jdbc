using System;
using System.Collections;
using System.Data.Common;
using System.Threading.Tasks;
using System.Xml.Linq;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    public class JdbcDataReader : DbDataReader
    {

        ResultSet rs;
        bool hasRows;
        int recordsAffected;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rs"></param>
        /// <param name="recordsAffected"></param>
        internal JdbcDataReader(ResultSet rs, int recordsAffected)
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
        /// Gets a value that indiciates whether this <see cref="JdbcDataReader"/> has one or more rows.
        /// </summary>
        public override bool HasRows => hasRows;

        /// <summary>
        /// Gets a value indicating whether this <see cref="JdbcDataReader"/> is closed.
        /// </summary>
        public override bool IsClosed => rs.isClosed();

        /// <summary>
        /// Gets the number of rows changed, inserted or deleted by the SQL statement.
        /// </summary>
        public override int RecordsAffected => recordsAffected;

        /// <summary>
        /// Gets an <see cref="IEnumerator"/> that can be used to iterate through the rows in the <see cref="JdbcDataReader"/>.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
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
                return typeof(DateTime);

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
        /// <exception cref="NotImplementedException"></exception>
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
                return rs.wasNull() ? DBNull.Value : v.getBytes(0, (int)v.length());
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
                return rs.wasNull() ? DBNull.Value : v.getSubString(0, (int)v.length());
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
                return rs.wasNull() ? DBNull.Value : v.getSubString(0, (int)v.length());
            }

            if (type == JDBCType.NULL.ordinal())
            {
                return DBNull.Value;
            }

            if (type == JDBCType.NUMERIC.ordinal())
            {
                throw new NotImplementedException();
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
                return rs.wasNull() ? DBNull.Value : XDocument.Parse(v.getString());
            }

            if (type == JDBCType.STRUCT.ordinal())
            {
                throw new NotSupportedException();
            }

            if (type == JDBCType.TIME.ordinal())
            {
                var v = rs.getTime(ordinal);
                return rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(v.getTime()).UtcDateTime;
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

            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                values[i] = GetValue(i);

            return rs.getMetaData().getColumnCount();
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
            var b = (byte[])GetValue(ordinal);
            if (b == null)
                throw new JdbcException("Null bytes.");

            var c = 0;
            for (int i = (int)dataOffset; i < b.Length && c < length; i++, c++)
                buffer[bufferOffset + i] = b[i];

            return c;
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
            var b = (char[])GetValue(ordinal);
            if (b == null)
                throw new JdbcException("Null chars.");

            var c = 0;
            for (int i = (int)dataOffset; i < b.Length && c < length; i++, c++)
                buffer[bufferOffset + i] = b[i];

            return c;
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
            return (string)GetValue(ordinal);
        }

        /// <summary>
        /// Advances the reader to the next record in the result set.
        /// </summary>
        /// <returns></returns>
        public override bool Read()
        {
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
        /// Releases all resources used by the current instance of the <see cref="JdbcDataReader"/> class.
        /// </summary>
        /// <param name="disposing"></param>
        protected override void Dispose(bool disposing)
        {
            if (rs != null)
            {
                rs.close();
                rs = null;
                hasRows = false;
                recordsAffected = 0;
            }

            base.Dispose(disposing);
        }

#if NETCOREAPP

        /// <summary>
        /// Asynchronously releases all resources used by the current instance of the <see cref="JdbcDataReader"/> class.
        /// </summary>
        /// <returns></returns>
        public override ValueTask DisposeAsync()
        {
            if (rs != null)
            {
                rs.close();
                rs = null;
                hasRows = false;
                recordsAffected = 0;
            }

            return base.DisposeAsync();
        }

#endif

    }

}
