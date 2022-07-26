using System.Collections.Generic;

using FluentAssertions;

using java.sql;

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace IKVM.Jdbc.Data.Tests
{

    [TestClass]
    public class JdbcConnectionTests
    {

        [TestMethod]
        public void Can_do_some_stuff()
        {
            var c = org.sqlite.JDBC.createConnection("jdbc:sqlite:sample.db", new java.util.Properties());
            using var cnn = new JdbcConnection(c);
            cnn.Open();
            using var cmd = cnn.CreateCommand();
            cmd.CommandText = "drop table if exists person";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "create table person (id integer, name string)";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into person values(1, 'leo')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "insert into person values(2, 'yui')";
            cmd.ExecuteNonQuery();
            cmd.CommandText = "select * from person";
            using var rdr = cmd.ExecuteReader();
            var list = new List<(long, string)>();
            while (rdr.Read())
                list.Add((rdr.GetFieldValue<long>(rdr.GetOrdinal("id")), rdr.GetString(1)));

            list.Should().HaveCount(2);
            list.Should().Contain(x => x.Item1 == 1 && x.Item2 == "leo");
            list.Should().Contain(x => x.Item1 == 2 && x.Item2 == "yui");
        }

    }

}
