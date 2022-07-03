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

        readonly ResultSet rs;
        private readonly int recordsAffected;
        readonly bool hasRows;

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

        public override object this[int ordinal] => GetValue(ordinal);

        public override object this[string name] => GetValue(GetOrdinal(name));

        public override int Depth => 0;

        public override int FieldCount => rs.getMetaData().getColumnCount();

        public override bool HasRows => hasRows;

        public override bool IsClosed => rs.isClosed();

        public override int RecordsAffected => recordsAffected;

        public override IEnumerator GetEnumerator()
        {
            throw new NotImplementedException();
        }

        public override Type GetFieldType(int ordinal)
        {
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
                return typeof(double);

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

        public override string GetDataTypeName(int ordinal)
        {
            return GetFieldType(ordinal).Name;
        }

        public override string GetName(int ordinal)
        {
            return rs.getMetaData().getColumnName(ordinal + 1);
        }

        public override int GetOrdinal(string name)
        {
            return rs.findColumn(name) - 1;
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
                var v = rs.getDouble(ordinal);
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

        public override int GetValues(object[] values)
        {
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                values[i] = GetValue(i);

            return rs.getMetaData().getColumnCount();
        }

        public override bool IsDBNull(int ordinal)
        {
            GetValue(ordinal);
            return rs.wasNull();
        }

        public override bool GetBoolean(int ordinal)
        {
            return (bool)GetValue(ordinal);
        }

        public override byte GetByte(int ordinal)
        {
            return (byte)GetValue(ordinal);
        }

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

        public override char GetChar(int ordinal)
        {
            return (char)GetValue(ordinal);
        }

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

        public override DateTime GetDateTime(int ordinal)
        {
            return (DateTime)GetValue(ordinal);
        }

        public override decimal GetDecimal(int ordinal)
        {
            return (decimal)GetValue(ordinal);
        }

        public override double GetDouble(int ordinal)
        {
            return (double)GetValue(ordinal);
        }

        public override float GetFloat(int ordinal)
        {
            return (float)GetValue(ordinal);
        }

        public override Guid GetGuid(int ordinal)
        {
            return Guid.Parse((string)GetValue(ordinal));
        }

        public override short GetInt16(int ordinal)
        {
            return (short)GetValue(ordinal);
        }

        public override int GetInt32(int ordinal)
        {
            return (int)GetValue(ordinal);
        }

        public override long GetInt64(int ordinal)
        {
            return (long)GetValue(ordinal);
        }

        public override string GetString(int ordinal)
        {
            return (string)GetValue(ordinal);
        }

        public override bool Read()
        {
            return rs.next();
        }

        public override bool NextResult()
        {
            return false;
        }

        protected override void Dispose(bool disposing)
        {
            rs.close();
            base.Dispose(disposing);
        }

#if NETCOREAPP

        public override ValueTask DisposeAsync()
        {
            if (rs.isClosed() == false)
                rs.close();

            return base.DisposeAsync();
        }

#endif

    }

}
