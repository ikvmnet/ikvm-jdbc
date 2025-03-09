using System;
using System.Buffers;
using System.IO;

using java.io;

namespace IKVM.Jdbc.Data
{

    class JdbcInputStream : Stream
    {

        readonly InputStream _input;
        readonly long _length;
        PushbackInputStream? _stream;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="input"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public JdbcInputStream(InputStream input, long length = -1)
        {
            _input = input ?? throw new ArgumentNullException(nameof(input));
            _length = length;
        }

        /// <summary>
        /// Initializes and retrieves the <see cref="PushbackReader"/>.
        /// </summary>
        PushbackInputStream Stream => _stream ??= new PushbackInputStream(_input, 1);

        /// <inheritdoc />
        public override bool CanRead => true;

        /// <inheritdoc />
        public override bool CanSeek => _length > -1;

        /// <inheritdoc />
        public override bool CanWrite => false;

        /// <inheritdoc />
        public override long Length => _length > -1 ? _length : throw new NotSupportedException();

        /// <inheritdoc />
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override int ReadByte()
        {
            try
            {
                return Stream.read();
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <inheritdoc />
        public override int Read(byte[] buffer, int offset, int count)
        {
            if (buffer is null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentOutOfRangeException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            if (buffer.Length - offset < count)
                throw new ArgumentOutOfRangeException("Invalid offset length.");

            try
            {
                var n = Stream.read(buffer, offset, count);
                if (n <= 0)
                    return 0;

                return n;
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

#if NET

        /// <inheritdoc />
        public override int Read(Span<byte> buffer)
        {
            var l = buffer.Length;
            var b = ArrayPool<byte>.Shared.Rent(l);

            try
            {
                var n = Stream.read(b, 0, l);
                if (n <= 0)
                    return 0;

                b.AsSpan().Slice(0, n).CopyTo(buffer);
                return n;
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
            finally
            {
                if (b is not null)
                    ArrayPool<byte>.Shared.Return(b);
            }
        }

#endif

        /// <inheritdoc />
        public override long Seek(long offset, SeekOrigin origin)
        {
            try
            {
                switch (origin)
                {
                    case SeekOrigin.Current:
                        return Stream.skip(offset);
                    case SeekOrigin.Begin:
                        Stream.reset();
                        return Stream.skip(offset);
                    case SeekOrigin.End:
                        throw new NotSupportedException("Seeking from the end is not supported.");
                    default:
                        throw new InvalidOperationException();
                }
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        /// <inheritdoc />
        public override void Flush()
        {
            throw new NotSupportedException();
        }

    }

}