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

        ResultSet _rs;
        int _recordsAffected;
        bool _hasRows;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rs"></param>
        /// <param name="recordsAffected"></param>
        internal JdbcDataReaderBase(ResultSet rs, int recordsAffected)
        {
            this._rs = rs ?? throw new ArgumentNullException(nameof(rs));
            this._recordsAffected = recordsAffected;
            this._hasRows = rs.isBeforeFirst();
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
        public override int FieldCount => _rs.getMetaData().getColumnCount();

        /// <summary>
        /// Gets a value that indiciates whether this <see cref="JdbcDataReaderBase"/> has one or more rows.
        /// </summary>
        public override bool HasRows => _hasRows;

        /// <summary>
        /// Gets a value indicating whether this <see cref="JdbcDataReaderBase"/> is closed.
        /// </summary>
        public override bool IsClosed => _rs.isClosed();

        /// <summary>
        /// Gets the number of rows changed, inserted or deleted by the SQL statement.
        /// </summary>
        public override int RecordsAffected => _recordsAffected;

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

            // java starts at 1
            ordinal++;
            var type = (JDBCType.__Enum)_rs.getMetaData().getColumnType(ordinal);
            return type switch
            {
                JDBCType.__Enum.ARRAY => typeof(System.Array),
                JDBCType.__Enum.BIGINT => typeof(long),
                JDBCType.__Enum.BINARY => typeof(byte[]),
                JDBCType.__Enum.BIT => typeof(bool),
                JDBCType.__Enum.BLOB => typeof(byte[]),
                JDBCType.__Enum.BOOLEAN => typeof(bool),
                JDBCType.__Enum.CHAR => typeof(string),
                JDBCType.__Enum.CLOB => typeof(string),
                JDBCType.__Enum.DATALINK => throw new NotSupportedException(),
                JDBCType.__Enum.DATE => typeof(DateTime),
                JDBCType.__Enum.DECIMAL => typeof(decimal),
                JDBCType.__Enum.DISTINCT => throw new NotImplementedException(),
                JDBCType.__Enum.DOUBLE => typeof(double),
                JDBCType.__Enum.FLOAT => typeof(float),
                JDBCType.__Enum.INTEGER => typeof(int),
                JDBCType.__Enum.JAVA_OBJECT => typeof(object),
                JDBCType.__Enum.LONGNVARCHAR => typeof(string),
                JDBCType.__Enum.LONGVARBINARY => typeof(byte[]),
                JDBCType.__Enum.LONGVARCHAR => typeof(string),
                JDBCType.__Enum.NCHAR => typeof(string),
                JDBCType.__Enum.NCLOB => typeof(string),
                JDBCType.__Enum.NULL => typeof(object),
                JDBCType.__Enum.NUMERIC => throw new NotImplementedException(),
                JDBCType.__Enum.NVARCHAR => typeof(string),
                JDBCType.__Enum.OTHER => throw new NotSupportedException(),
                JDBCType.__Enum.REAL => typeof(float),
                JDBCType.__Enum.REF => throw new NotSupportedException(),
                JDBCType.__Enum.REF_CURSOR => throw new NotSupportedException(),
                JDBCType.__Enum.ROWID => throw new NotImplementedException(),
                JDBCType.__Enum.SMALLINT => typeof(short),
                JDBCType.__Enum.SQLXML => typeof(XDocument),
                JDBCType.__Enum.STRUCT => throw new NotSupportedException(),
                JDBCType.__Enum.TIME => typeof(TimeSpan),
                JDBCType.__Enum.TIMESTAMP => typeof(DateTime),
                JDBCType.__Enum.TIMESTAMP_WITH_TIMEZONE => typeof(DateTimeOffset),
                JDBCType.__Enum.TIME_WITH_TIMEZONE => typeof(DateTimeOffset),
                JDBCType.__Enum.TINYINT => typeof(byte),
                JDBCType.__Enum.VARBINARY => typeof(byte[]),
                JDBCType.__Enum.VARCHAR => typeof(string),
                _ => throw new NotSupportedException(),
            };
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

            // java starts at 1
            ordinal++;
            return _rs.getMetaData().getColumnTypeName(ordinal);
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

            // java starts at 1
            ordinal++;
            return _rs.getMetaData().getColumnName(ordinal);
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

            return _rs.findColumn(name) is int i && i > 0 ? i - 1 : -1;
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
            var type = (JDBCType.__Enum)_rs.getMetaData().getColumnType(ordinal);

            switch (type)
            {
                case JDBCType.__Enum.ARRAY:
                    {
                        var v = _rs.getArray(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v.getArray();
                    }
                case JDBCType.__Enum.BIGINT:
                    {
                        var v = _rs.getLong(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.BINARY:
                    {
                        var v = _rs.getBytes(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.BIT:
                    {
                        var v = _rs.getBoolean(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.BLOB:
                    {
                        var v = _rs.getBlob(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.BOOLEAN:
                    {
                        var v = _rs.getBoolean(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.CHAR:
                    {
                        var v = _rs.getString(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.CLOB:
                    {
                        var v = _rs.getClob(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.DATALINK:
                    throw new NotSupportedException();
                case JDBCType.__Enum.DATE:
                    {
                        var v = _rs.getDate(ordinal);
                        return _rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(v.getTime()).UtcDateTime;
                    }
                case JDBCType.__Enum.DECIMAL:
                    {
                        var v = _rs.getBigDecimal(ordinal);
                        return _rs.wasNull() ? DBNull.Value : decimal.Parse(v.toString());
                    }
                case JDBCType.__Enum.DISTINCT:
                    throw new NotImplementedException();
                case JDBCType.__Enum.DOUBLE:
                    {
                        var v = _rs.getDouble(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.FLOAT:
                    {
                        var v = _rs.getFloat(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.INTEGER:
                    {
                        var v = _rs.getInt(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.JAVA_OBJECT:
                    {
                        var v = _rs.getObject(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.LONGNVARCHAR:
                    {
                        var v = _rs.getNString(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.LONGVARBINARY:
                    {
                        var v = _rs.getBytes(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.LONGVARCHAR:
                    {
                        var v = _rs.getString(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.NCHAR:
                    {
                        var v = _rs.getNString(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.NCLOB:
                    {
                        var v = _rs.getNClob(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.NULL:
                    return DBNull.Value;
                case JDBCType.__Enum.NUMERIC:
                    throw new NotImplementedException();
                case JDBCType.__Enum.NVARCHAR:
                    {
                        var v = _rs.getNString(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.OTHER:
                    throw new NotSupportedException();
                case JDBCType.__Enum.REAL:
                    {
                        var v = _rs.getFloat(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.REF:
                    throw new NotSupportedException();
                case JDBCType.__Enum.REF_CURSOR:
                    throw new NotSupportedException();
                case JDBCType.__Enum.ROWID:
                    throw new NotImplementedException();
                case JDBCType.__Enum.SMALLINT:
                    {
                        var v = _rs.getShort(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.SQLXML:
                    {
                        var v = _rs.getSQLXML(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.STRUCT:
                    throw new NotSupportedException();
                case JDBCType.__Enum.TIME:
                    {
                        var v = _rs.getTime(ordinal);
                        return _rs.wasNull() ? DBNull.Value : TimeSpan.FromMilliseconds(v.getTime());
                    }
                case JDBCType.__Enum.TIMESTAMP:
                    {
                        var v = _rs.getTime(ordinal);
                        return _rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(v.getTime()).UtcDateTime;
                    }
                case JDBCType.__Enum.TIMESTAMP_WITH_TIMEZONE:
                    {
                        var v = _rs.getTime(ordinal);
                        return _rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(v.getTime());
                    }
                case JDBCType.__Enum.TIME_WITH_TIMEZONE:
                    {
                        var v = _rs.getTime(ordinal);
                        return _rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(v.getTime());
                    }
                case JDBCType.__Enum.TINYINT:
                    {
                        var v = _rs.getByte(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.VARBINARY:
                    {
                        var v = _rs.getBytes(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                case JDBCType.__Enum.VARCHAR:
                    {
                        var v = _rs.getString(ordinal);
                        return _rs.wasNull() ? DBNull.Value : v;
                    }
                default:
                    throw new NotSupportedException();
            }
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

            var n = _rs.getMetaData().getColumnCount();
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
            return _rs.wasNull();
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
            if (_rs == null)
                throw new JdbcException("JdbcReader is closed.");

            return _rs.next();
        }

        /// <summary>
        /// Advances the reader to the next result when reading the results of a batch of statements.
        /// </summary>
        /// <returns></returns>
        public override bool NextResult()
        {
            _rs = null;
            _hasRows = false;
            _recordsAffected = 0;
            return false;
        }

        /// <summary>
        /// Closes the <see cref="JdbcDataReader"/> object.
        /// </summary>
        public override void Close()
        {
            if (_rs != null)
            {
                _rs.close();
                _rs = null;
                _hasRows = false;
                _recordsAffected = 0;
            }

            base.Close();
        }

    }

}
