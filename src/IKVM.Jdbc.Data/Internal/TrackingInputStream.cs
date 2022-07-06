using System;
using System.IO;

namespace IKVM.Jdbc.Data.Internal
{

    /// <summary>
    /// Represents a .NET stream over a Java <see cref="java.io.InputStream"/>.
    /// </summary>
    class TrackingInputStream : Stream
    {

        readonly PositionInputStream stream;
        readonly int length;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="length"></param>
        public TrackingInputStream(java.io.InputStream stream, int length)
        {
            this.stream = new PositionInputStream(stream ?? throw new ArgumentNullException(nameof(stream)));
            this.length = length;
        }

        public override bool CanRead => true;

        public override bool CanSeek => stream.markSupported();

        public override bool CanWrite => false;

        public override long Length => length > -1 ? length : throw new NotSupportedException();

        public override long Position
        {
            get => stream.getPosition();
            set => throw new NotSupportedException();
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }
    }

}
