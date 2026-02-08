using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcDataReaderTimestampWithTimeZoneTests
    {

        static JdbcDataReaderTimestampWithTimeZoneTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.h2.Driver).Assembly);
            GC.KeepAlive(org.h2.Driver.load());

            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.hsqldb.jdbc.JDBCDriver).Assembly);
            GC.KeepAlive(org.hsqldb.jdbc.JDBCDriver.driverInstance);
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
        public void CanGetAsDateTimeOffset()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TIMESTAMP WITH TIME ZONE '2005-12-31 23:59:59-10:00'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.ThrowsExactly<JdbcUnsupportedTypeException>(() => rdr.GetFieldValue<DateTimeOffset>(0));
        }

        [TestMethod]
        public void CanGetAsDateTime()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TIMESTAMP WITH TIME ZONE '2005-12-31 23:59:59-10:00'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.ThrowsExactly<JdbcUnsupportedTypeException>(() => rdr.GetFieldValue<DateTime>(0));
        }

        [TestMethod]
        public void CanGetAsDateTimeOffset_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_TIMESTAMP_TZ('2005-12-31 23:59:59 -10:00', 'YYYY-MM-DD HH:MI:SS TZ') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTimeOffset(2005, 12, 31, 23, 59, 59, TimeSpan.FromHours(-10)), rdr.GetFieldValue<DateTimeOffset>(0));
        }

        [TestMethod]
        public void CanGetAsDateTime_Jdbc42()
        {
            using var cnn = CreateHSQLDBTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TO_TIMESTAMP_TZ('2005-12-31 23:59:59 -10:00','YYYY-MM-DD HH:MI:SS TZ') FROM INFORMATION_SCHEMA.SYSTEM_USERS";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            var datetime = rdr.GetFieldValue<DateTime>(0);
            Assert.AreEqual(new DateTime(2006, 1, 1, 9, 59, 59), datetime);
        }

    }

}
