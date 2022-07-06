using java.io;

namespace IKVM.Jdbc.Data.Internal
{

    /// <summary>
    /// Maintains the position within a <see cref="TrackingInputStream"/>.
    /// </summary>
    class PositionInputStream : FilterInputStream
    {

        long pos = 0;
        long mark_ = 0;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="stream"></param>
        public PositionInputStream(java.io.InputStream stream) :
            base(stream)
        {

        }


        /// <summary>
        /// Gets the stream position.
        /// </summary>
        /// <remarks>
        /// Eventually, the position will roll over to a negative number.
        /// Reading 1 Tb per second, this would occur after approximately three
        /// months. Applications should account for this possibility in their
        /// design.
        /// </remarks>
        /// <returns></returns>
        public long getPosition()
        {
            lock (this)
                return pos;
        }

        public override int read()
        {
            lock (this)
            {
                int b = base.read();
                if (b >= 0)
                    pos += 1;
                return b;
            }
        }

        public override int read(byte[] b, int off, int len)
        {
            lock (this)
            {
                int n = base.read(b, off, len);
                if (n > 0)
                    pos += n;
                return n;
            }
        }

        public override long skip(long skip)
        {
            lock (this)
            {
                long n = base.skip(skip);
                if (n > 0)
                    pos += n;
                return n;
            }
        }

        public override void mark(int readlimit)
        {
            lock (this)
            {
                base.mark(readlimit);
                mark_ = pos;
            }
        }

        public override void reset()
        {
            lock (this)
            {
                /* A call to reset can still succeed if mark is not supported, but the 
                 * resulting stream position is undefined, so it's not allowed here. */
                if (!markSupported())
                    throw new IOException("Mark not supported.");

                base.reset();
                pos = mark_;
            }
        }

    }

}
