using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcDataReaderDateTests
    {

        static JdbcDataReaderDateTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.h2.Driver).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.hsqldb.jdbc.JDBCDriver).Assembly);
        }

        JdbcConnection CreateH2TestConnection()
        {
            return new JdbcConnection("jdbc:h2:mem:sample");
        }

        JdbcConnection CreateHSQLDBTestConnection()
        {
            return new JdbcConnection("jdbc:hsqldb:mem:sample");
        }

        [TestMethod]
        public void CanGetAsDateTime()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT DATE '2022-10-15'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTime(2022, 10, 15), rdr.GetFieldValue<DateTime>(0));
        }

        [TestMethod]
        public void CanGetAsDateTime_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_DATE('2022-10-15', 'YYYY-MM-DD') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTime(2022, 10, 15), rdr.GetFieldValue<DateTime>(0));
        }

        [TestMethod]
        public void CanGetAsDateTimeOffset()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT DATE '2022-10-15'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTimeOffset(new DateTime(2022, 10, 15), TimeSpan.Zero), rdr.GetFieldValue<DateTimeOffset>(0));
        }

        [TestMethod]
        public void CanGetAsDateTimeOffset_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_DATE('2022-10-15', 'YYYY-MM-DD') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTimeOffset(new DateTime(2022, 10, 15), TimeSpan.Zero), rdr.GetFieldValue<DateTimeOffset>(0));
        }

#if NET

        [TestMethod]
        public void CanGetAsDateOnly()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT DATE '2022-10-15'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateOnly(2022, 10, 15), rdr.GetFieldValue<DateOnly>(0));
        }

        [TestMethod]
        public void CanGetAsDateOnly_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_DATE('2022-10-15', 'YYYY-MM-DD') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateOnly(2022, 10, 15), rdr.GetFieldValue<DateOnly>(0));
        }

#endif

    }

}
