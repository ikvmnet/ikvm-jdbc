using java.math;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcTypeConversionTests
    {

        [TestMethod]
        public void CanConvertBigDecimalToDecimal32()
        {
            var big = new BigDecimal(65535);
            var dec = JdbcTypeConversion.ToDecimal(big);
            Assert.AreEqual(65535, dec);
        }

        [TestMethod]
        public void CanConvertBigDecimalToDecimal32Negative()
        {
            var big = new BigDecimal(-65535);
            var dec = JdbcTypeConversion.ToDecimal(big);
            Assert.AreEqual(-65535, dec);
        }

        [TestMethod]
        public void CanConvertBigDecimalToDecimal64()
        {
            var big = new BigDecimal(1281281281231);
            var dec = JdbcTypeConversion.ToDecimal(big);
            Assert.AreEqual(1281281281231, dec);
        }

        [TestMethod]
        public void CanConvertBigDecimalToDecimal64Negative()
        {
            var big = new BigDecimal(-1281281281231);
            var dec = JdbcTypeConversion.ToDecimal(big);
            Assert.AreEqual(-1281281281231, dec);
        }

    }

}
