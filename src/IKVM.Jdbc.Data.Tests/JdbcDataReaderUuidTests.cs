using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcDataReaderUuidTests
    {

        static JdbcDataReaderUuidTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.h2.Driver).Assembly);
            GC.KeepAlive(org.h2.Driver.load());
        }

        JdbcConnection CreateH2TestConnection()
        {
            return new JdbcConnection("jdbc:h2:mem:sample");
        }

        [TestMethod]
        public void CanGetGuid()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CAST('75281326-D2E0-497D-AB70-5C3183E22A38' AS UUID)";
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
            cmd.CommandText = "SELECT CAST('75281326-D2E0-497D-AB70-5C3183E22A38' AS UUID)";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new Guid("75281326-D2E0-497D-AB70-5C3183E22A38"), rdr.GetFieldValue<Guid>(0));
        }

    }

}
