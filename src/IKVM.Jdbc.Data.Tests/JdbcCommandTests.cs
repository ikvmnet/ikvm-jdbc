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
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.sqlite.JDBC).Assembly);
            org.sqlite.SQLiteJDBCLoader.initialize();
        }

        JdbcConnection CreateSqliteTestConnection()
        {
            return new JdbcConnection("jdbc:sqlite:sample.db");
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
        public void CanGetInsertCountFromExecuteNonQuery()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetInsertCountFromExecuteNonQuery";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetInsertCountFromExecuteNonQuery (id integer)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetInsertCountFromExecuteNonQuery values (null)";
            Assert.AreEqual(1, cmd.ExecuteNonQuery());
        }

        [TestMethod]
        public void CanGetInsertCountFromExecuteReader()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetInsertCountFromExecuteNonQuery";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetInsertCountFromExecuteNonQuery (id integer)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetInsertCountFromExecuteNonQuery values (null)";
            Assert.AreEqual(1, cmd.ExecuteReader().RecordsAffected);
        }

        [TestMethod]
        public void CanGetUpdateCountFromExecuteNonQuery()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetUpdateCountFromExecuteNonQuery";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetUpdateCountFromExecuteNonQuery (id integer, t char(1))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetUpdateCountFromExecuteNonQuery values (1, 'A')";
            Assert.AreEqual(1, cmd.ExecuteReader().RecordsAffected);
            cmd.CommandText = "update CanGetUpdateCountFromExecuteNonQuery SET t = 'B' WHERE id == 1";
            Assert.AreEqual(1, cmd.ExecuteReader().RecordsAffected);
            cmd.CommandText = "update CanGetUpdateCountFromExecuteNonQuery SET t = 'B' WHERE id == 2";
            Assert.AreEqual(0, cmd.ExecuteReader().RecordsAffected);
        }

        [TestMethod]
        public void CanGetAutoGeneratedKeys()
        {
            using var cnn = CreateSqliteTestConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanGetUpdateCountFromExecuteNonQuery";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanGetUpdateCountFromExecuteNonQuery (id integer, t char(1))";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanGetUpdateCountFromExecuteNonQuery values (1, 'A')";
            var rdr = cmd.ExecuteReader();
            Assert.AreEqual(1, rdr.RecordsAffected);
            Assert.IsTrue(rdr.Read());
            var id = rdr.GetInt32(0);
            Assert.AreNotEqual(0, id);

        }

    }

}
