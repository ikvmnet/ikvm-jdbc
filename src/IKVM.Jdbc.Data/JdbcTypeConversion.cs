using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using com.sun.org.apache.xerces.@internal.impl.dv.xs;

using java.math;

namespace IKVM.Jdbc.Data
{

    static class JdbcTypeConversion
    {

        /// <summary>
        /// Converts the <see cref="BigDecimal"/> value to a <see cref="decimal"/>. This fails with <see cref="ArgumentOutOfRangeException"/> for <see cref="BigDecimal"/> instances with more than 96 bits of integer data, and outside the .NET decimal scale limitation of 28.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public static decimal ToDecimal(BigDecimal value)
        {
            var unscaled = value.unscaledValue().abs();
            if (unscaled.bitLength() > 96)
                throw new ArgumentOutOfRangeException("BigDecimal too big for CLR decimal type.");

            var scale = value.scale();
            if (scale > 28 || scale < 0)
                throw new ArgumentOutOfRangeException("BigDecimal scale exceeds CLR decimal scale limit of 0-28");

            // copy the biginteger bytes to local memory, expanded to 16 bytes
            var imported = (Span<byte>)stackalloc byte[16];
            var unscaledArray = unscaled.toByteArray();
            unscaledArray.AsSpan().CopyTo(imported[(16 - unscaledArray.Length)..]);

            // read integer components
            var l32 = BinaryPrimitives.ReadInt32BigEndian(imported[^4..]);
            var m32 = BinaryPrimitives.ReadInt32BigEndian(imported[^8..]);
            var h32 = BinaryPrimitives.ReadInt32BigEndian(imported[^12..]);

            return new decimal(l32, m32, h32, value.signum() == -1, (byte)scale);
        }

    }

}
