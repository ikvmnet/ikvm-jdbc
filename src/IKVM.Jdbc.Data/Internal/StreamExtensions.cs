using System;
using System.IO;

namespace IKVM.Jdbc.Data.Internal
{

    static class StreamExtensions
    {

        /// <summary>
        /// Reads all bytes of the specified stream to an array.
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="bufferSize"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        public static byte[] ReadAllBytes(this Stream stream, int bufferSize = 81920)
        {
            if (stream is null)
                throw new ArgumentNullException(nameof(stream));

            var m = new MemoryStream();
            stream.CopyTo(m, bufferSize);
            return m.ToArray();
        }

    }

}
