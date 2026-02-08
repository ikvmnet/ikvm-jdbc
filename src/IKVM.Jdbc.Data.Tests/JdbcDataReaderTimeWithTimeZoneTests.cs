using System;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcDataReaderTimeWithTimeZoneTests
    {

        static JdbcDataReaderTimeWithTimeZoneTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.h2.Driver).Assembly);
        }

        JdbcConnection CreateH2TestConnection()
        {
            return new JdbcConnection("jdbc:h2:mem:sample");
        }


        [TestMethod]
        public void CanGetAsDateTimeOffset()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT TIME WITH TIME ZONE '15:04:01+10'";
            using var rdr = cmd.ExecuteReader();
            Assert.IsTrue(rdr.Read());
            Assert.AreEqual(1, rdr.FieldCount);
            Assert.AreEqual(new DateTimeOffset(1, 1, 1, 15, 4, 1, TimeSpan.FromHours(10)), rdr.GetFieldValue<DateTimeOffset>(0));
        }

    }

}
