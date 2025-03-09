﻿using System;
using System.Buffers;
using System.Collections;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO;
using System.Xml.Linq;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    public abstract class JdbcDataReaderBase : DbDataReader
    {

        const int DEFAULT_BUFFER_SIZE = 1024;

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
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                return _rs.getMetaData().getColumnType(column) switch
                {
                    Types.ARRAY => typeof(System.Array),
                    Types.BIGINT => typeof(long),
                    Types.BINARY => typeof(byte[]),
                    Types.BIT => typeof(bool),
                    Types.BLOB => typeof(byte[]),
                    Types.BOOLEAN => typeof(bool),
                    Types.CHAR => typeof(string),
                    Types.CLOB => typeof(string),
                    Types.DATALINK => throw new NotSupportedException(),
                    Types.DATE => typeof(DateTime),
                    Types.DECIMAL => typeof(decimal),
                    Types.DISTINCT => throw new NotImplementedException(),
                    Types.DOUBLE => typeof(double),
                    Types.FLOAT => typeof(float),
                    Types.INTEGER => typeof(int),
                    Types.JAVA_OBJECT => typeof(object),
                    Types.LONGNVARCHAR => typeof(string),
                    Types.LONGVARBINARY => typeof(byte[]),
                    Types.LONGVARCHAR => typeof(string),
                    Types.NCHAR => typeof(string),
                    Types.NCLOB => typeof(string),
                    Types.NULL => typeof(object),
                    Types.NUMERIC => throw new NotImplementedException(),
                    Types.NVARCHAR => typeof(string),
                    Types.OTHER => throw new NotSupportedException(),
                    Types.REAL => typeof(float),
                    Types.REF => throw new NotSupportedException(),
                    Types.REF_CURSOR => throw new NotSupportedException(),
                    Types.ROWID => throw new NotImplementedException(),
                    Types.SMALLINT => typeof(short),
                    Types.SQLXML => typeof(XDocument),
                    Types.STRUCT => throw new NotSupportedException(),
                    Types.TIME => typeof(TimeSpan),
                    Types.TIMESTAMP => typeof(DateTime),
                    Types.TIMESTAMP_WITH_TIMEZONE => typeof(DateTimeOffset),
                    Types.TIME_WITH_TIMEZONE => typeof(DateTimeOffset),
                    Types.TINYINT => typeof(byte),
                    Types.VARBINARY => typeof(byte[]),
                    Types.VARCHAR => typeof(string),
                    _ => throw new NotSupportedException(),
                };
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
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
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                return _rs.getMetaData().getColumnTypeName(column);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
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
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                return _rs.getMetaData().getColumnName(column);
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
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

            try
            {
                return _rs.findColumn(name) is int i && i > 0 ? i - 1 : -1;
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public override object GetValue(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.ARRAY:
                        var array_ = _rs.getArray(column);
                        return _rs.wasNull() ? DBNull.Value : array_.getArray();
                    case Types.BIGINT:
                        var long_ = _rs.getLong(column);
                        return _rs.wasNull() ? DBNull.Value : long_;
                    case Types.BINARY:
                        var binary_ = _rs.getBytes(column);
                        return _rs.wasNull() ? DBNull.Value : binary_;
                    case Types.BIT:
                        var bit_ = _rs.getBoolean(column);
                        return _rs.wasNull() ? DBNull.Value : bit_;
                    case Types.BLOB:
                        var blob_ = _rs.getBytes(column);
                        return _rs.wasNull() ? DBNull.Value : blob_;
                    case Types.BOOLEAN:
                        var bool_ = _rs.getBoolean(column);
                        return _rs.wasNull() ? DBNull.Value : bool_;
                    case Types.CHAR:
                        var char_ = _rs.getString(column);
                        return _rs.wasNull() ? DBNull.Value : char_;
                    case Types.CLOB:
                        var clob_ = _rs.getString(column);
                        return _rs.wasNull() ? DBNull.Value : clob_;
                    case Types.DATALINK:
                        throw new NotSupportedException();
                    case Types.DATE:
                        var date_ = _rs.getDate(column);
                        return _rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(date_.getTime()).UtcDateTime;
                    case Types.DECIMAL:
                        var decimal_ = _rs.getBigDecimal(column);
                        return _rs.wasNull() ? DBNull.Value : decimal.Parse(decimal_.toString());
                    case Types.DISTINCT:
                        throw new NotImplementedException();
                    case Types.DOUBLE:
                        var double_ = _rs.getDouble(column);
                        return _rs.wasNull() ? DBNull.Value : double_;
                    case Types.FLOAT:
                        var float_ = _rs.getFloat(column);
                        return _rs.wasNull() ? DBNull.Value : float_;
                    case Types.INTEGER:
                        var integer_ = _rs.getInt(column);
                        return _rs.wasNull() ? DBNull.Value : integer_;
                    case Types.JAVA_OBJECT:
                        var object_ = _rs.getObject(column);
                        return _rs.wasNull() ? DBNull.Value : object_;
                    case Types.LONGNVARCHAR:
                        var longnvarchar_ = _rs.getString(column);
                        return _rs.wasNull() ? DBNull.Value : longnvarchar_;
                    case Types.LONGVARBINARY:
                        var longvarbinary_ = _rs.getBytes(column);
                        return _rs.wasNull() ? DBNull.Value : longvarbinary_;
                    case Types.LONGVARCHAR:
                        var longvarchar_ = _rs.getString(column);
                        return _rs.wasNull() ? DBNull.Value : longvarchar_;
                    case Types.NCHAR:
                        var nchar_ = _rs.getString(column);
                        return _rs.wasNull() ? DBNull.Value : nchar_;
                    case Types.NCLOB:
                        var nclob_ = _rs.getString(column);
                        return _rs.wasNull() ? DBNull.Value : nclob_;
                    case Types.NULL:
                        return DBNull.Value;
                    case Types.NUMERIC:
                        throw new NotImplementedException();
                    case Types.NVARCHAR:
                        var nvarchar_ = _rs.getString(column);
                        return _rs.wasNull() ? DBNull.Value : nvarchar_;
                    case Types.OTHER:
                        throw new NotSupportedException();
                    case Types.REAL:
                        var real_ = _rs.getFloat(column);
                        return _rs.wasNull() ? DBNull.Value : real_;
                    case Types.REF:
                        throw new NotSupportedException();
                    case Types.REF_CURSOR:
                        throw new NotSupportedException();
                    case Types.ROWID:
                        throw new NotImplementedException();
                    case Types.SMALLINT:
                        var smallint_ = _rs.getShort(column);
                        return _rs.wasNull() ? DBNull.Value : smallint_;
                    case Types.SQLXML:
                        var sqlxml_ = _rs.getSQLXML(column);
                        return _rs.wasNull() ? DBNull.Value : XDocument.Parse(sqlxml_.getString());
                    case Types.STRUCT:
                        throw new NotSupportedException();
                    case Types.TIME:
                        var time_ = _rs.getTime(column);
                        return _rs.wasNull() ? DBNull.Value : TimeSpan.FromMilliseconds(time_.getTime());
                    case Types.TIMESTAMP:
                        var timestamp_ = _rs.getTime(column);
                        return _rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(timestamp_.getTime()).UtcDateTime;
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        var timestampwithtimezone_ = _rs.getTime(column);
                        return _rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(timestampwithtimezone_.getTime());
                    case Types.TIME_WITH_TIMEZONE:
                        var timewithtimezone_ = _rs.getTime(column);
                        return _rs.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(timewithtimezone_.getTime());
                    case Types.TINYINT:
                        var tinyint_ = _rs.getByte(column);
                        return _rs.wasNull() ? DBNull.Value : tinyint_;
                    case Types.VARBINARY:
                        var varbinary_ = _rs.getBytes(column);
                        return _rs.wasNull() ? DBNull.Value : varbinary_;
                    case Types.VARCHAR:
                        var varchar_ = _rs.getString(column);
                        return _rs.wasNull() ? DBNull.Value : varchar_;
                    default:
                        throw new NotSupportedException();
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
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

            try
            {
                var n = _rs.getMetaData().getColumnCount();
                for (int i = 0; i < n; i++)
                    values[i] = GetValue(i);

                return n;
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Gets a value that indicates whether the column contains non-existent or null values.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override bool IsDBNull(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                GetValue(ordinal);
                return _rs.wasNull();
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="bool"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override bool GetBoolean(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.BOOLEAN:
                        var bool_ = _rs.getBoolean(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return bool_;
                    case Types.BIT:
                        var bit_ = _rs.getBoolean(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return bit_;
                    case Types.TINYINT:
                        var byte_ = _rs.getByte(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return byte_ != 0;
                    case Types.SMALLINT:
                        var short_ = _rs.getShort(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return short_ != 0;
                    case Types.INTEGER:
                        var int_ = _rs.getInt(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return int_ != 0;
                    case Types.BIGINT:
                        var long_ = _rs.getLong(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return long_ != 0;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Boolean.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="byte"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override byte GetByte(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.BOOLEAN:
                        var bool_ = _rs.getBoolean(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return bool_ ? (byte)1 : (byte)0;
                    case Types.BIT:
                        var bit_ = _rs.getBoolean(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return bit_ ? (byte)1 : (byte)0;
                    case Types.TINYINT:
                        var byte_ = _rs.getByte(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return byte_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Byte.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column ordinal as a byte array.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public byte[]? GetBytes(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        var b = _rs.getBytes(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return b;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Byte[].");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
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
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (dataOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));
            if (buffer is null)
                throw new ArgumentNullException(nameof(buffer));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        var stream = _rs.getBinaryStream(column);
                        if (_rs.wasNull())
                        {
                            return 0;
                        }
                        else
                        {
                            // skip until we consume offset bytes
                            while (dataOffset > 0)
                                dataOffset -= stream.skip(dataOffset);

                            int n = 0; // total read
                            int i = 0; // current read

                            // read up to buffer size, from buffer offset, or remaining space available, until end
                            while ((i = stream.read(buffer, n + bufferOffset, Math.Min(DEFAULT_BUFFER_SIZE, buffer.Length - n))) != -1)
                                n += i;

                            return n;
                        }
                    default:
                        throw new JdbcException($"Could not retrieve bytes.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
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
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (dataOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        var stream = _rs.getBinaryStream(column);
                        if (_rs.wasNull())
                        {
                            return 0;
                        }
                        else
                        {
                            // skip until we consume offset bytes
                            while (dataOffset > 0)
                                dataOffset -= stream.skip(dataOffset);

                            var b = ArrayPool<byte>.Shared.Rent(DEFAULT_BUFFER_SIZE);

                            try
                            {
                                int n = 0; // total read
                                int i = 0; // current read

                                // read up to buffer size, or remaining space available, until end
                                while ((i = stream.read(b, 0, Math.Min(DEFAULT_BUFFER_SIZE, buffer.Length - n))) != -1)
                                {
                                    b.AsSpan().Slice(0, i).CopyTo(buffer.Slice(n));
                                    n += i;
                                }

                                return n;
                            }
                            finally
                            {
                                if (b is not null)
                                    ArrayPool<byte>.Shared.Return(b);
                            }
                        }
                    default:
                        throw new JdbcException($"Could not retrieve bytes.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column as a single <see cref="char"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override char GetChar(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                        var char_ = _rs.getByte(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return (char)char_;
                    case Types.NCHAR:
                        var nchar_ = _rs.getByte(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return (char)nchar_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Char.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column ordinal as a char array.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public char[]? GetChars(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CLOB:
                        var b = _rs.getString(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return b.ToCharArray();
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Byte[].");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
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
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (dataOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));
            if (buffer is null)
                throw new ArgumentNullException(nameof(buffer));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                        var stream = _rs.getCharacterStream(column);
                        if (_rs.wasNull())
                        {
                            return 0;
                        }
                        else
                        {
                            // skip until we consume offset bytes
                            while (dataOffset > 0)
                                dataOffset -= stream.skip(dataOffset);

                            int n = 0; // total read
                            int i = 0; // current read

                            // read up to buffer size, from buffer offset, or remaining space available, until end
                            while ((i = stream.read(buffer, n + bufferOffset, Math.Min(DEFAULT_BUFFER_SIZE, buffer.Length - n))) != -1)
                                n += i;

                            return n;
                        }
                    default:
                        throw new JdbcException($"Could not retrieve chars.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
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
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (dataOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                        var stream = _rs.getCharacterStream(column);
                        if (_rs.wasNull())
                        {
                            return 0;
                        }
                        else
                        {
                            // skip until we consume offset bytes
                            while (dataOffset > 0)
                                dataOffset -= stream.skip(dataOffset);

                            var b = ArrayPool<char>.Shared.Rent(DEFAULT_BUFFER_SIZE);

                            try
                            {
                                int n = 0; // total read
                                int i = 0; // current read

                                // read up to buffer size, or remaining space available, until end
                                while ((i = stream.read(b, 0, Math.Min(DEFAULT_BUFFER_SIZE, buffer.Length - n))) != -1)
                                {
                                    b.AsSpan().Slice(0, i).CopyTo(buffer.Slice(n));
                                    n += i;
                                }

                                return n;
                            }
                            finally
                            {
                                if (b is not null)
                                    ArrayPool<char>.Shared.Return(b);
                            }
                        }
                    default:
                        throw new JdbcException($"Could not retrieve bytes.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <inheritdoc />
        public override DateTime GetDateTime(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            return (DateTime)GetValue(ordinal);
        }

        /// <inheritdoc />
        public override decimal GetDecimal(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.DECIMAL:
                        var decimal_ = _rs.getBigDecimal(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return decimal.Parse(decimal_.ToString());
                    case Types.DOUBLE:
                        var double_ = _rs.getDouble(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return (decimal)double_;
                    case Types.FLOAT:
                        var float_ = _rs.getFloat(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return (decimal)float_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Decimal.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override double GetDouble(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.DOUBLE:
                        var double_ = _rs.getDouble(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return double_;
                    case Types.FLOAT:
                        var float_ = _rs.getFloat(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return float_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Double.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override float GetFloat(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.FLOAT:
                        var float_ = _rs.getFloat(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return float_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Single.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override Guid GetGuid(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            return Guid.Parse((string)GetValue(ordinal));
        }

        /// <inheritdoc />
        public override short GetInt16(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.SMALLINT:
                        var short_ = _rs.getShort(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return short_;
                    case Types.TINYINT:
                        var byte_ = _rs.getByte(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return byte_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Int16.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override int GetInt32(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.INTEGER:
                        var int_ = _rs.getInt(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return int_;
                    case Types.SMALLINT:
                        var short_ = _rs.getShort(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return short_;
                    case Types.TINYINT:
                        var byte_ = _rs.getByte(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return byte_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Int32.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override long GetInt64(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.BIGINT:
                        var long_ = _rs.getLong(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return long_;
                    case Types.INTEGER:
                        var int_ = _rs.getInt(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return int_;
                    case Types.SMALLINT:
                        var short_ = _rs.getShort(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return short_;
                    case Types.TINYINT:
                        var byte_ = _rs.getByte(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return byte_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into Int64.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override string GetString(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                        var char_ = (char)_rs.getByte(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return char_.ToString();
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CLOB:
                    case Types.NCLOB:
                        var string_ = _rs.getString(column);
                        if (_rs.wasNull())
                            throw new SqlNullValueException();

                        return string_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {_rs.getMetaData().getColumnType(column)} into String.");
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override Stream GetStream(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        var binaryStream = _rs.getBinaryStream(column);
                        return _rs.wasNull() ? Stream.Null : new JdbcInputStream(binaryStream, binaryStream.available());
                    default:
                        return base.GetStream(ordinal);
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override TextReader GetTextReader(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= _rs.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (_rs.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                        var char_ = _rs.getByte(column);
                        return _rs.wasNull() ? TextReader.Null : new StringReader(((char)char_).ToString());
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                        var reader_ = _rs.getCharacterStream(column);
                        return _rs.wasNull() ? TextReader.Null : new JdbcTextReader(reader_);
                    case Types.CLOB:
                        var clob = _rs.getClob(column);
                        return _rs.wasNull() ? TextReader.Null : new JdbcClobTextReader(_rs.getClob(column));
                    case Types.NCLOB:
                        var nclob = _rs.getNClob(column);
                        return _rs.wasNull() ? TextReader.Null : new JdbcClobTextReader(_rs.getClob(column));
                    default:
                        return base.GetTextReader(ordinal);
                }
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override bool Read()
        {
            try
            {
                return _rs.next();
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override bool NextResult()
        {
            try
            {
                _rs?.close();
                _hasRows = false;
                _recordsAffected = 0;
                return false;
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <inheritdoc />
        public override void Close()
        {
            try
            {
                _rs?.close();
                _hasRows = false;
                _recordsAffected = 0;
                base.Close();
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

    }

}
