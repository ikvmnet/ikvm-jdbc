using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.IO;

using java.sql;

namespace IKVM.Jdbc.Data
{

    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    public abstract class JdbcDataReaderBase : DbDataReader
    {

        readonly JdbcCommand _command;
        readonly Statement _statement;
        readonly bool _getGeneratedKeys;

        IEnumerator<(int UpdateCount, ResultSet? ResultSet)> _iter;
        int _updateCount;
        ResultSet? _resultSet;
        JdbcResultSetAdapter? _resultSetAdapter;
        bool _hasRows;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="command"></param>
        /// <param name="statement"></param>
        /// <param name="getGeneratedKeys"></param>
        internal JdbcDataReaderBase(JdbcCommand command, Statement statement, bool getGeneratedKeys)
        {
            _command = command ?? throw new ArgumentNullException(nameof(command));
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _getGeneratedKeys = getGeneratedKeys;

            _iter = GetStatementIter().GetEnumerator();
            if (AdvanceNextResultSet() == false)
                throw new InvalidOperationException("No active result set.");
        }

        /// <summary>
        /// Advances the iter to the next result set.
        /// </summary>
        bool AdvanceNextResultSet()
        {
            if (_command.Connection is null)
                throw new InvalidOperationException();

            try
            {
                // close existing result set
                if (_resultSet is not null)
                    _resultSet.close();

                // clear existing variables
                _resultSet = null;
                _resultSetAdapter = null;
                _updateCount = -1;
                _hasRows = false;

                // check that we have another result to process
                if (_iter.MoveNext() == false)
                    return false;

                _updateCount = _iter.Current.UpdateCount;
                if (_iter.Current.ResultSet != null)
                {
                    _resultSet = _iter.Current.ResultSet;
                    _resultSetAdapter = new JdbcResultSetAdapter(_command.Connection.JdbcVersion, _resultSet);
                    _hasRows = _resultSet.isBeforeFirst();
                }

                return true;
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

        /// <summary>
        /// Gets the active result set.
        /// </summary>
        public ResultSet JdbcResultSet => _resultSet ?? throw new InvalidOperationException("No active result set.");

        /// <summary>
        /// Gets the active result set.
        /// </summary>
        JdbcResultSetAdapter ResultSetAdapter => _resultSetAdapter ?? throw new InvalidOperationException("No active result set.");

        /// <inheritdoc />
        public override object this[int ordinal] => ResultSetAdapter[ordinal];

        /// <inheritdoc />
        public override object this[string name] => ResultSetAdapter[name];

        /// <inheritdoc />
        public override int Depth => 0;

        /// <inheritdoc />
        public override int FieldCount => _resultSetAdapter != null ? _resultSetAdapter.FieldCount : 0;

        /// <inheritdoc />
        public override bool HasRows => _hasRows;

        /// <inheritdoc />
        public override bool IsClosed => _resultSetAdapter != null ? _resultSetAdapter.IsClosed : false;

        /// <inheritdoc />
        public override int RecordsAffected => _updateCount;

        /// <inheritdoc />
        public override IEnumerator GetEnumerator()
        {
            return new JdbcEnumerator(this);
        }

        /// <inheritdoc />
        public override Type GetFieldType(int ordinal)
        {
            return ResultSetAdapter.GetFieldType(ordinal);
        }

        /// <inheritdoc />
        public override string GetDataTypeName(int ordinal)
        {
            return ResultSetAdapter.GetDataTypeName(ordinal);
        }

        /// <inheritdoc />
        public override string GetName(int ordinal)
        {
            return ResultSetAdapter.GetName(ordinal);
        }

        /// <inheritdoc />
        public override int GetOrdinal(string name)
        {
            return ResultSetAdapter.GetOrdinal(name);
        }

        /// <inheritdoc />
        public override object GetProviderSpecificValue(int ordinal)
        {
            return ResultSetAdapter.GetProviderSpecificValue(ordinal);
        }

        public override Type GetProviderSpecificFieldType(int ordinal)
        {
            return ResultSetAdapter.GetProviderSpecificFieldType(ordinal);
        }

        /// <inheritdoc />
        public override object GetValue(int ordinal)
        {
            return ResultSetAdapter.GetValue(ordinal);
        }

        /// <inheritdoc />
        public override T GetFieldValue<T>(int ordinal)
        {
            return ResultSetAdapter.GetFieldValue<T>(ordinal);
        }

        /// <inheritdoc />
        public override int GetValues(object[] values)
        {
            return ResultSetAdapter.GetValues(values);
        }

        /// <inheritdoc />
        public override bool IsDBNull(int ordinal)
        {
            return ResultSetAdapter.IsDBNull(ordinal);
        }

        /// <inheritdoc />
        public override bool GetBoolean(int ordinal)
        {
            return ResultSetAdapter.GetBoolean(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="SByte"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public sbyte GetSByte(int ordinal)
        {
            return ResultSetAdapter.GetSByte(ordinal);
        }

        /// <inheritdoc />
        public override byte GetByte(int ordinal)
        {
            return ResultSetAdapter.GetByte(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column ordinal as a byte array.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public byte[] GetBytes(int ordinal)
        {
            return ResultSetAdapter.GetBytes(ordinal);
        }

        /// <inheritdoc />
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            return ResultSetAdapter.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
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
            return ResultSetAdapter.GetBytes(ordinal, dataOffset, buffer);
        }

        /// <inheritdoc />
        public override char GetChar(int ordinal)
        {
            return ResultSetAdapter.GetChar(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column ordinal as a char array.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public char[]? GetChars(int ordinal)
        {
            return ResultSetAdapter.GetChars(ordinal);
        }

        /// <inheritdoc />
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            return ResultSetAdapter.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
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
            return ResultSetAdapter.GetChars(ordinal, dataOffset, buffer);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateTimeOffset"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            return ResultSetAdapter.GetDateTimeOffset(ordinal);
        }

        /// <inheritdoc />
        public override DateTime GetDateTime(int ordinal)
        {
            return ResultSetAdapter.GetDateTime(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="TimeSpan"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public TimeSpan GetTimeSpan(int ordinal)
        {
            return ResultSetAdapter.GetTimeSpan(ordinal);
        }

#if NET

        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateOnly"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public DateOnly GetDateOnly(int ordinal)
        {
            return ResultSetAdapter.GetDateOnly(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="TimeOnly"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public TimeOnly GetTimeOnly(int ordinal)
        {
            return ResultSetAdapter.GetTimeOnly(ordinal);
        }

#endif

        /// <inheritdoc />
        public override decimal GetDecimal(int ordinal)
        {
            return ResultSetAdapter.GetDecimal(ordinal);
        }

        /// <inheritdoc />
        public override double GetDouble(int ordinal)
        {
            return ResultSetAdapter.GetDouble(ordinal);
        }

        /// <inheritdoc />
        public override float GetFloat(int ordinal)
        {
            return ResultSetAdapter.GetFloat(ordinal);
        }

        /// <inheritdoc />
        public override Guid GetGuid(int ordinal)
        {
            return ResultSetAdapter.GetGuid(ordinal);
        }

        /// <inheritdoc />
        public override short GetInt16(int ordinal)
        {
            return ResultSetAdapter.GetInt16(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="ushort"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public ushort GetUInt16(int ordinal)
        {
            return ResultSetAdapter.GetUInt16(ordinal);
        }

        /// <inheritdoc />
        public override int GetInt32(int ordinal)
        {
            return ResultSetAdapter.GetInt32(ordinal);
        }

        /// Gets the value of the specified column as a <see cref="uint"/>.
        public uint GetUInt32(int ordinal)
        {
            return ResultSetAdapter.GetUInt32(ordinal);
        }

        /// <inheritdoc />
        public override long GetInt64(int ordinal)
        {
            return ResultSetAdapter.GetInt64(ordinal);
        }

        /// Gets the value of the specified column as a <see cref="ulong"/>.
        public ulong GetUInt64(int ordinal)
        {
            return ResultSetAdapter.GetUInt64(ordinal);
        }

        /// <inheritdoc />
        public override string GetString(int ordinal)
        {
            return ResultSetAdapter.GetString(ordinal);
        }

        /// <inheritdoc />
        public override Stream GetStream(int ordinal)
        {
            return ResultSetAdapter.GetStream(ordinal);
        }

        /// <inheritdoc />
        public override TextReader GetTextReader(int ordinal)
        {
            return ResultSetAdapter.GetTextReader(ordinal);
        }

        /// <inheritdoc />
        public override bool Read()
        {
            return ResultSetAdapter.Read();
        }

        /// <summary>
        /// Returns an <see cref="IEnumerable{ResultSet}"/> over the complete results available on a statement, including any appended results.
        /// </summary>
        /// <returns></returns>
        IEnumerable<(int, ResultSet?)> GetStatementIter()
        {
            do
            {
                var updateCount = _statement.getUpdateCount();
                if (updateCount == -1)
                {
                    yield return (-1, _statement.getResultSet());
                }
                else
                {
                    if (_getGeneratedKeys)
                    {
                        yield return (updateCount, _statement.getGeneratedKeys());
                    }
                    else
                    {
                        yield return (updateCount, null);
                    }
                }
            }
            while (_statement.getMoreResults());
        }

        /// <inheritdoc />
        public override bool NextResult()
        {
            return AdvanceNextResultSet();
        }

        /// <inheritdoc />
        public override void Close()
        {
            try
            {
                _resultSet?.close();
                _hasRows = false;
                base.Close();
            }
            catch (SQLException e)
            {
                throw new JdbcException(e);
            }
        }

    }

}
