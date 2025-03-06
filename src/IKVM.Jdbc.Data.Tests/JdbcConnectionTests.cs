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

        JdbcConnection CreateTestConnection()
        {
            return new JdbcConnection(org.sqlite.JDBC.createConnection("jdbc:sqlite:sample.db", new java.util.Properties()));
        }

        [TestMethod]
        public void CanConnect()
        {
            using var cnn = CreateTestConnection();
            cnn.Open();
            cnn.State.Should().Be(System.Data.ConnectionState.Open);
        }

        [TestMethod]
        public async Task CanConnectAsync()
        {
            using var cnn = CreateTestConnection();
            await cnn.OpenAsync();
            cnn.State.Should().Be(System.Data.ConnectionState.Open);
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
            cmd.CommandText = "create table CanExecuteReader (id integer, name string)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanExecuteReader values(1, 'leo')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanExecuteReader values(2, 'yui')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into CanExecuteReader values(3, NULL)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from CanExecuteReader";
            using var rdr = cmd.ExecuteReader();
            var list = new List<(long, object)>();
            while (rdr.Read())
                list.Add((rdr.GetFieldValue<long>(rdr.GetOrdinal("id")), rdr.GetValue(1)));

            list.Should().HaveCount(3);
            list.Should().Contain(x => x.Item1 == 1 && (string)x.Item2 == "leo");
            list.Should().Contain(x => x.Item1 == 2 && (string)x.Item2 == "yui");
            list.Should().Contain(x => x.Item1 == 3 && x.Item2 == DBNull.Value);
        }

        [TestMethod]
        public async Task CanExecuteReaderAsync()
        {
            using var cnn = CreateTestConnection();
            await cnn.OpenAsync();
            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists CanExecuteReaderAsync";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "create table CanExecuteReaderAsync (id integer, name string)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "insert into CanExecuteReaderAsync values(1, 'leo')";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "insert into CanExecuteReaderAsync values(2, 'yui')";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "insert into CanExecuteReaderAsync values(3, NULL)";
            await cmd.ExecuteNonQueryAsync();
            cmd.CommandText = "select * from CanExecuteReaderAsync";
            using var rdr = await cmd.ExecuteReaderAsync();
            var list = new List<(long, object)>();
            while (await rdr.ReadAsync())
                list.Add((await rdr.GetFieldValueAsync<long>(rdr.GetOrdinal("id")), rdr.GetValue(1)));

            list.Should().HaveCount(3);
            list.Should().Contain(x => x.Item1 == 1 && (string)x.Item2 == "leo");
            list.Should().Contain(x => x.Item1 == 2 && (string)x.Item2 == "yui");
            list.Should().Contain(x => x.Item1 == 3 && x.Item2 == DBNull.Value);
        }

    }

}
