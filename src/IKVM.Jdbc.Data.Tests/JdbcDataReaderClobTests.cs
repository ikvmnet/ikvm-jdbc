using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcDataReaderClobTests
    {

        static JdbcDataReaderClobTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.h2.Driver).Assembly);
            GC.KeepAlive(org.h2.Driver.load());
        }

        JdbcConnection CreateH2TestConnection()
        {
            return new JdbcConnection("jdbc:h2:mem:sample");
        }

        [TestMethod]
        public void CanGetString()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST('THIS IS A STRING' AS CLOB)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);

            var str = rdr.GetString(0);
            Assert.IsNotNull(str);
            Assert.HasCount(16, str);
            Assert.AreEqual("THIS IS A STRING", str);
        }

        [TestMethod]
        public void CanGetChars()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST('THIS IS A STRING' AS CLOB)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);

            var chars = rdr.GetChars(0);
            Assert.IsNotNull(chars);
            Assert.HasCount(16, chars);
            Assert.AreEqual("THIS IS A STRING", new string(chars));
        }

        [TestMethod]
        public void CanGetCharsToArray()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST('AAAAAAAAAAAAAAAA' AS CLOB)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);

            var buffer = new char[32];
            var length = rdr.GetChars(0, 0, buffer, 0, buffer.Length);
            Assert.AreEqual(16, length);

            for (int i = 0; i < 16; i++)
                Assert.AreEqual('A', buffer[i]);
            for (int i = 16; i < 32; i++)
                Assert.AreEqual((char)0, buffer[i]);
        }

        [TestMethod]
        public void CanGetCharsToSpan()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST('AAAAAAAAAAAAAAAA' AS CLOB)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);

            var buffer = (Span<char>)stackalloc char[32];
            var length = rdr.GetChars(0, 0, buffer);
            Assert.AreEqual(16, length);

            for (int i = 0; i < 16; i++)
                Assert.AreEqual('A', buffer[i]);
            for (int i = 16; i < 32; i++)
                Assert.AreEqual((char)0, buffer[i]);
        }

        [TestMethod]
        public void CanGetTextReader()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST('AAAAAAAAAAAAAAAA' AS CLOB)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);

            using var stm = rdr.GetTextReader(0);
            var text = stm.ReadToEnd();
            Assert.AreEqual(16, text.Length);
            Assert.AreEqual("AAAAAAAAAAAAAAAA", text);
        }

        [TestMethod]
        public void CanGetGuid()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST(CAST('75281326-D2E0-497D-AB70-5C3183E22A38' AS UUID) AS CLOB)";
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
            cmd.CommandText = "SELECT CAST(CAST('75281326-D2E0-497D-AB70-5C3183E22A38' AS UUID) AS CLOB)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new Guid("75281326-D2E0-497D-AB70-5C3183E22A38"), rdr.GetFieldValue<Guid>(0));
        }

    }

}
