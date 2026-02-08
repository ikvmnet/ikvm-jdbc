using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcDataReaderTimestampTests
    {

        static JdbcDataReaderTimestampTests()
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
            cmd.CommandText = "SELECT TIMESTAMP '2005-12-31 23:59:59'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTime(2005, 12, 31, 23, 59, 59), rdr.GetFieldValue<DateTime>(0));
        }

        [TestMethod]
        public void CanGetAsDateTime_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_TIMESTAMP('2005-12-31 23:59:59', 'YYYY-MM-DD HH:MI:SS') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTime(2005, 12, 31, 23, 59, 59), rdr.GetFieldValue<DateTime>(0));
        }

        [TestMethod]
        public void CanGetAsDateTimeOffset()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TIMESTAMP '2005-12-31 23:59:59'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTimeOffset(2005, 12, 31, 23, 59, 59, TimeSpan.Zero), rdr.GetFieldValue<DateTimeOffset>(0));
        }

        [TestMethod]
        public void CanGetAsDateTimeOffset_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_TIMESTAMP('2005-12-31 23:59:59', 'YYYY-MM-DD HH:MI:SS') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTimeOffset(2005, 12, 31, 23, 59, 59, TimeSpan.Zero), rdr.GetFieldValue<DateTimeOffset>(0));
        }

        [TestMethod]
        public void CanGetAsTimeSpan()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TIMESTAMP '1970-01-01 12:32:01'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new TimeSpan(12, 32, 1), rdr.GetFieldValue<TimeSpan>(0));
        }

        [TestMethod]
        public void CanGetAsTimeSpan_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_TIMESTAMP('2005-12-31 12:32:01', 'YYYY-MM-DD HH:MI:SS') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new TimeSpan(12, 32, 1), rdr.GetFieldValue<TimeSpan>(0));
        }

#if NET

        [TestMethod]
        public void CanGetAsDateOnly()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TIMESTAMP '2005-12-31'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateOnly(2005, 12, 31), rdr.GetFieldValue<DateOnly>(0));
        }


        [TestMethod]
        public void CanGetAsDateOnly_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_TIMESTAMP('2005-12-31 12:32:01', 'YYYY-MM-DD HH:MI:SS') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateOnly(2005, 12, 31), rdr.GetFieldValue<DateOnly>(0));
        }

        [TestMethod]
        public void CanGetAsTimeOnly()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TIMESTAMP '1970-01-01 12:32:01'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new TimeOnly(12, 32, 1), rdr.GetFieldValue<TimeOnly>(0));
        }

        [TestMethod]
        public void CanGetAsTimeOnly_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_TIMESTAMP('2005-12-31 12:32:01', 'YYYY-MM-DD HH:MI:SS') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new TimeOnly(12, 32, 1), rdr.GetFieldValue<TimeOnly>(0));
        }

#endif

    }

}
