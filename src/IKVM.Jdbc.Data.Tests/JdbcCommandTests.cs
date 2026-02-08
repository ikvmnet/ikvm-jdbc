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
            GC.KeepAlive(org.h2.Driver.load());

            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.sqlite.JDBC).Assembly);
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
