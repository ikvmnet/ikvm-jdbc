using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcDataReaderDecimalTests
    {

        static JdbcDataReaderDecimalTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.h2.Driver).Assembly);
        }

        JdbcConnection CreateH2TestConnection()
        {
            return new JdbcConnection("jdbc:h2:mem:sample");
        }

        [TestMethod]
        public void CanGetDecimal()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST(1024.01 AS DECIMAL)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(1024.01m, rdr.GetDecimal(0));
        }

        [TestMethod]
        public void CanGetAsDecimal()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST(1024.01 AS DECIMAL)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(1024.01m, rdr.GetFieldValue<decimal>(0));
        }

    }

}
