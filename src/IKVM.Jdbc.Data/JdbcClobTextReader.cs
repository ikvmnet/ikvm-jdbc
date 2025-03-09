using System;
using System.IO;

using java.sql;

namespace IKVM.Jdbc.Data
{

    class JdbcClobTextReader : TextReader
    {

        readonly Clob _clob;
        JdbcTextReader? _reader;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="clob"></param>
        public JdbcClobTextReader(Clob clob)
        {
            _clob = clob ?? throw new ArgumentNullException(nameof(clob));
        }

        /// <summary>
        /// Initializes and retrieves the <see cref="JdbcTextReader"/>.
        /// </summary>
        JdbcTextReader Reader => _reader ??= new JdbcTextReader(_clob.getCharacterStream());

        /// <inheritdoc />
        public override int Peek() => Reader.Peek();

        /// <inheritdoc />
        public override int Read() => Reader.Read();

        /// <inheritdoc />
        public override int Read(char[] buffer, int index, int count) => Reader.Read(buffer, index, count);

#if NET

        /// <inheritdoc />
        public override int Read(Span<char> buffer) => Reader.Read(buffer);

#endif

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _reader?.Dispose();
                _clob.free();
            }

            base.Dispose(disposing);
        }

    }

}
