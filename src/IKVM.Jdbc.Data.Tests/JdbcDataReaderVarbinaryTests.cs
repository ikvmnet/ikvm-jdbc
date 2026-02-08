using System;

using IKVM.Jdbc.Data.Internal;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcDataReaderVarbinaryTests
    {

        static JdbcDataReaderVarbinaryTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.h2.Driver).Assembly);
        }

        JdbcConnection CreateH2TestConnection()
        {
            return new JdbcConnection("jdbc:h2:mem:sample");
        }

        [TestMethod]
        public void CanGetBytesToArray()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST(X'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff' AS VARBINARY)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);

            var buffer = new byte[32];
            var length = rdr.GetBytes(0, 0, buffer, 0, buffer.Length);
            Assert.AreEqual(16, length);

            for (int i = 0; i < 16; i++)
                Assert.AreEqual((byte)(240 + i), buffer[i]);
            for (int i = 16; i < 32; i++)
                Assert.AreEqual(0, buffer[i]);
        }

        [TestMethod]
        public void CanGetBytesToSpan()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST(X'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff' AS VARBINARY)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);

            var buffer = (Span<byte>)stackalloc byte[32];
            var length = rdr.GetBytes(0, 0, buffer);
            Assert.AreEqual(16, length);

            for (int i = 0; i < 16; i++)
                Assert.AreEqual((byte)(240 + i), buffer[i]);
            for (int i = 16; i < 32; i++)
                Assert.AreEqual(0, buffer[i]);
        }

        [TestMethod]
        public void CanGetBytesToStream()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST(X'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff' AS VARBINARY)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);

            using var stm = rdr.GetStream(0);
            var buffer = stm.ReadAllBytes(4);
            Assert.AreEqual(16, buffer.Length);

            for (int i = 0; i < 16; i++)
                Assert.AreEqual((byte)(240 + i), buffer[i]);
        }

        [TestMethod]
        public void CanGetGuid()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST(CAST('75281326-D2E0-497D-AB70-5C3183E22A38' AS UUID) AS VARBINARY)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new Guid("75281326-D2E0-497D-AB70-5C3183E22A38"), rdr.GetGuid(0));
        }

        [TestMethod]
        public void CanGetAsGuid()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST(CAST('75281326-D2E0-497D-AB70-5C3183E22A38' AS UUID) AS VARBINARY)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new Guid("75281326-D2E0-497D-AB70-5C3183E22A38"), rdr.GetFieldValue<Guid>(0));
        }

    }

}
