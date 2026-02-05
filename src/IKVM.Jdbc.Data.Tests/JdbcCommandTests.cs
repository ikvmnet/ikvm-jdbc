using System;

using FluentAssertions;

using IKVM.Jdbc.Data.Internal;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcCommandTests
    {

        static JdbcCommandTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.h2.Driver).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.sqlite.JDBC).Assembly);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.postgresql.jdbc.PgConnection).Assembly);
        }

        JdbcConnection CreateSqliteTestConnection()
        {
            return new JdbcConnection("jdbc:sqlite:sample.db");
        }

        JdbcConnection CreateH2TestConnection()
        {
            return new JdbcConnection("jdbc:h2:mem:sample");
        }

        [TestMethod]
        public void CanGetColumnNames()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();
            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetColumnNames";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetColumnNames (id integer, name string)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetColumnNames";
            using var rdr = cmd.ExecuteReader();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("name");
        }

        [TestMethod]
        public void CanGetPrimitiveFieldValuesByType()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetPrimitiveFieldValuesByType";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetPrimitiveFieldValuesByType (id integer)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetPrimitiveFieldValuesByType values (1)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetPrimitiveFieldValuesByType";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(1);
            rdr.GetName(0).Should().Be("id");
            rdr.GetFieldValue<int>(0).Should().Be(1);
        }

        [TestMethod]
        public void CanGetNullablePrimitiveFieldValuesByType()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetPrimitiveFieldValuesByType";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetPrimitiveFieldValuesByType (id integer)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetPrimitiveFieldValuesByType values (null)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetPrimitiveFieldValuesByType";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(1);
            rdr.GetName(0).Should().Be("id");
            rdr.GetFieldValue<int?>(0).Should().BeNull();
        }

        [TestMethod]
        public void CanGetStringAsDateTime()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "select current_timestamp";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(1);
            rdr.GetFieldValue<DateTime>(0).Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(1));
        }

        [TestMethod]
        public void CanGetDateAsDateTime()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CURRENT_TIMESTAMP";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(1);
            rdr.GetFieldValue<DateTime>(0).Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromMinutes(1));
        }

#if NET

        [TestMethod]
        public void CanGetDateAsDateOnly()
        {
            using var cnn = CreateH2TestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT CURRENT_DATE";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(1);
            rdr.GetFieldValue<DateOnly>(0).Should().Be(DateOnly.FromDateTime(DateTime.Today.Date));
        }

        [TestMethod]
        public void CanGetStringAsDateOnly()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "select current_date";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(1);
            rdr.GetFieldValue<DateOnly>(0).Should().Be(DateOnly.FromDateTime(DateTime.Today.Date));
        }

#endif

        [TestMethod]
        public void CanGetBytesToArray()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetBytesToArray";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetBytesToArray (id integer, data blob)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetBytesToArray values (1, x'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetBytesToArray";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            var buffer = new byte[32];
            rdr.GetBytes(1, 0, buffer, 0, buffer.Length).Should().Be(16);

            for (int i = 0; i < 16; i++)
                buffer[i].Should().Be((byte)(240 + i));
            for (int i = 16; i < 32; i++)
                buffer[i].Should().Be((byte)0);
        }

        [TestMethod]
        public void CanGetBytesToSpan()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetBytesToSpan";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetBytesToSpan (id integer, data blob)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetBytesToSpan values (1, x'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetBytesToSpan";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            var buffer = (Span<byte>)stackalloc byte[32];
            rdr.GetBytes(1, 0, buffer).Should().Be(16);

            for (int i = 0; i < 16; i++)
                buffer[i].Should().Be((byte)(240 + i));
            for (int i = 16; i < 32; i++)
                buffer[i].Should().Be((byte)0);
        }

        [TestMethod]
        public void CanGetBytesToStream()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetBytesToStream";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetBytesToStream (id integer, data blob)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetBytesToStream values (1, x'f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetBytesToStream";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            using var stm = rdr.GetStream(1);
            var buffer = stm.ReadAllBytes(4);
            buffer.Length.Should().Be(16);

            for (int i = 0; i < 16; i++)
                buffer[i].Should().Be((byte)(240 + i));
        }

        [TestMethod]
        public void CanGetCharsToArray()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetCharsToArray";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetCharsToArray (id integer, data text)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetCharsToArray values (1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetCharsToArray";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            var buffer = new char[32];
            rdr.GetChars(1, 0, buffer, 0, buffer.Length).Should().Be(26);

            for (int i = 0; i < 26; i++)
                buffer[i].Should().Be((char)((byte)'A' + i));
            for (int i = 26; i < 32; i++)
                buffer[i].Should().Be((char)0);
        }

        [TestMethod]
        public void CanGetCharsToSpan()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetCharsToSpan";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetCharsToSpan (id integer, data text)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetCharsToSpan values (1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetCharsToSpan";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            var buffer = (Span<char>)stackalloc char[32];
            rdr.GetChars(1, 0, buffer).Should().Be(26);

            for (int i = 0; i < 26; i++)
                buffer[i].Should().Be((char)((byte)'A' + i));
            for (int i = 26; i < 32; i++)
                buffer[i].Should().Be((char)0);
        }

        [TestMethod]
        public void CanGetTextToChars()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetTextToChars";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetTextToChars (id integer, data text)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetTextToChars values (1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetTextToChars";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            var stm = rdr.GetChars(1);
            stm.Should().NotBeNull();
            stm.Length.Should().Be(26);

            for (int i = 0; i < 26; i++)
                stm[i].Should().Be((char)((byte)'A' + i));
        }

        [TestMethod]
        public void CanGetTextToReader()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetTextToReader";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetTextToReader (id integer, data text)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetTextToReader values (1, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanGetTextToReader";
            using var rdr = cmd.ExecuteReader();
            rdr.Read().Should().BeTrue();
            rdr.FieldCount.Should().Be(2);
            rdr.GetName(0).Should().Be("id");
            rdr.GetName(1).Should().Be("data");

            using var stm = rdr.GetTextReader(1);
            var buffer = stm.ReadToEnd();
            buffer.Length.Should().Be(26);

            for (int i = 0; i < 26; i++)
                buffer[i].Should().Be((char)((byte)'A' + i));
        }

        //[TestMethod]
        //public void CanReturnGeneratedKeys()
        //{
        //    using var cnn = new JdbcConnection(DriverManager.getConnection("jdbc:postgresql:test", "postgres", "10241024"));
        //    cnn.Open();

        //    using var cmd = cnn.CreateCommand();
        //    cmd.CommandType = System.Data.CommandType.Text;
        //    cmd.CommandText = "INSERT INTO test (name) VALUES (?) -- :GetGeneratedKeys";
        //    cmd.Parameters.AddWithValue("1", "BOB");
        //    using var rdr = cmd.ExecuteReader();
        //    while (rdr.Read())
        //    {
        //        Console.WriteLine(rdr[0]);
        //    }
        //}

    }

}
