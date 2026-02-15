using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using FluentAssertions;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcConnectionTests
    {

        static JdbcConnectionTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.sqlite.JDBC).Assembly);
            org.sqlite.SQLiteJDBCLoader.initialize();
        }

        JdbcConnection CreateTestConnection()
        {
            return new JdbcConnection("jdbc:sqlite:sample.db");
        }

        [TestMethod]
        public void CanOpen()
        {
            using var cnn = CreateTestConnection();
            Assert.AreEqual(System.Data.ConnectionState.Closed, cnn.State);
            cnn.Open();
            Assert.AreEqual(System.Data.ConnectionState.Open, cnn.State);
        }

        [TestMethod]
        public void CanOpenAndCloseAndOpenAndClose()
        {
            using var cnn = CreateTestConnection();
            Assert.AreEqual(System.Data.ConnectionState.Closed, cnn.State);
            cnn.Open();
            Assert.AreEqual(System.Data.ConnectionState.Open, cnn.State);
            cnn.Close();
            Assert.AreEqual(System.Data.ConnectionState.Closed, cnn.State);
            cnn.Open();
            Assert.AreEqual(System.Data.ConnectionState.Open, cnn.State);
            cnn.Close();
            Assert.AreEqual(System.Data.ConnectionState.Closed, cnn.State);
        }

        [TestMethod]
        public void CanConnect()
        {
            using var cnn = CreateTestConnection();
            cnn.Open();
            Assert.AreEqual(System.Data.ConnectionState.Open, cnn.State);
        }

        [TestMethod]
        public async Task CanConnectAsync()
        {
            using var cnn = CreateTestConnection();
            await cnn.OpenAsync();
            Assert.AreEqual(System.Data.ConnectionState.Open, cnn.State);
        }

        [TestMethod]
        public void CanExecuteNonQuery()
        {
            using var cnn = CreateTestConnection();
            cnn.Open();
            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT 0 WHERE 0";
            cmd.ExecuteNonQuery().Should().Be(-1);
        }

        [TestMethod]
        public async Task CanExecuteNonQueryAsync()
        {
            using var cnn = CreateTestConnection();
            await cnn.OpenAsync();
            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "SELECT 0 WHERE 0";
            (await cmd.ExecuteNonQueryAsync()).Should().Be(-1);
        }

        [TestMethod]
        public void CanExecuteReader()
        {
            using var cnn = CreateTestConnection();
            cnn.Open();
            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanExecuteReader";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table CanExecuteReader (id integer, name text)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanExecuteReader values(1, 'leo')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanExecuteReader values(2, 'yui')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanExecuteReader values(3, NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanExecuteReader";
            using var rdr = cmd.ExecuteReader();
            var list = new List<(long, string?)>();
            while (rdr.Read())
                list.Add((rdr.GetInt64(0), rdr.IsDBNull(1) ? null : rdr.GetString(1)));

            list.Should().HaveCount(3);
            list.Should().Contain(x => x.Item1 == 1 && x.Item2 == "leo");
            list.Should().Contain(x => x.Item1 == 2 && x.Item2 == "yui");
            list.Should().Contain(x => x.Item1 == 3 && x.Item2 == null);
        }

        [TestMethod]
        public async Task CanExecuteReaderAsync()
        {
            using var cnn = CreateTestConnection();
            await cnn.OpenAsync();
            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanExecuteReaderAsync";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "create table CanExecuteReaderAsync (id integer, name text)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "insert into CanExecuteReaderAsync values(1, 'leo')";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "insert into CanExecuteReaderAsync values(2, 'yui')";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "insert into CanExecuteReaderAsync values(3, NULL)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "select * from CanExecuteReaderAsync";
            using var rdr = await cmd.ExecuteReaderAsync();
            var list = new List<(long, string?)>();
            while (await rdr.ReadAsync())
                list.Add((rdr.GetInt64(0), rdr.IsDBNull(1) ? null : rdr.GetString(1)));

            list.Should().HaveCount(3);
            list.Should().Contain(x => x.Item1 == 1 && x.Item2 == "leo");
            list.Should().Contain(x => x.Item1 == 2 && x.Item2 == "yui");
            list.Should().Contain(x => x.Item1 == 3 && x.Item2 == null);
        }

    }

}
